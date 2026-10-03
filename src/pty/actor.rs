#[cfg(unix)]
mod unix;

#[cfg(unix)]
pub(crate) use unix::*;

#[cfg(windows)]
mod windows {
    use std::io::{Read, Write};
    use std::sync::{mpsc as std_mpsc, Arc, Mutex};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use portable_pty::{MasterPty, PtySize};
    use tokio::sync::mpsc;
    use tracing::{debug, warn};

    pub(crate) struct PtyReadResult {
        pub terminal_responses: Vec<Bytes>,
    }

    type ReadCallback = Box<dyn FnMut(&[u8]) -> PtyReadResult + Send + 'static>;
    type ReaderExitCallback = Box<dyn FnOnce() + Send + 'static>;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct PtyResize {
        rows: u16,
        cols: u16,
        cell_width_px: u32,
        cell_height_px: u32,
    }

    struct PtyResizeRequest {
        resize: PtyResize,
        terminal_responses: Vec<Bytes>,
    }

    pub(crate) struct PtyIoActorConfig {
        pub pane_id: u32,
        pub master: Box<dyn MasterPty + Send>,
        pub initially_quiesced: bool,
        pub on_read: ReadCallback,
        pub on_reader_exit: Option<ReaderExitCallback>,
    }

    enum PtyIoDataCommand {
        WriteUserInput(Bytes, crate::terminal::lifecycle_binding::InputGuard),
        SubmitUserInput {
            guard: crate::terminal::lifecycle_binding::InputGuard,
            text: Bytes,
            enter: Bytes,
            delay: Duration,
            deadline: Option<Instant>,
            reply: std_mpsc::Sender<std::io::Result<()>>,
        },
    }

    enum PtyIoWriteCommand {
        Write(Bytes),
        UserInput(Bytes, crate::terminal::lifecycle_binding::InputGuard),
        SubmissionPart {
            guard: crate::terminal::lifecycle_binding::InputGuard,
            bytes: Bytes,
            deadline: Option<Instant>,
            reply: std_mpsc::Sender<std::io::Result<()>>,
        },
    }

    enum PtyIoControlCommand {
        Resize(PtyResizeRequest),
        Shutdown,
    }

    #[derive(Clone)]
    pub(crate) struct PtyIoActorHandle {
        pub(crate) lifecycle: crate::terminal::lifecycle_binding::LifecycleBinding,
        data_tx: mpsc::Sender<PtyIoDataCommand>,
        control_tx: std_mpsc::Sender<PtyIoControlCommand>,
        write_tx: std_mpsc::Sender<PtyIoWriteCommand>,
        response_order: Arc<Mutex<()>>,
        accepting: Arc<Mutex<bool>>,
    }

    impl PtyIoActorHandle {
        pub(crate) fn try_write_user_input(&self,
        bytes: Bytes,) -> Result<(), mpsc::error::TrySendError<Bytes>> {
            self.lifecycle.invalidate();
            if !*self
                .accepting
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
            {
                return Err(mpsc::error::TrySendError::Closed(bytes));
            }
            self.data_tx
                .try_send(PtyIoDataCommand::WriteUserInput(bytes, crate::terminal::lifecycle_binding::InputGuard::Untracked(self.lifecycle.clone())))
                .map_err(|err| match err {
                    mpsc::error::TrySendError::Full(command) => {
                        let PtyIoDataCommand::WriteUserInput(bytes, _) = command else {
                            unreachable!("queued write returned another command")
                        };
                        mpsc::error::TrySendError::Full(bytes)
                    }
                    mpsc::error::TrySendError::Closed(command) => {
                        let PtyIoDataCommand::WriteUserInput(bytes, _) = command else {
                            unreachable!("queued write returned another command")
                        };
                        mpsc::error::TrySendError::Closed(bytes)
                    }
                })
        }

        pub(crate) fn queue_user_input_submission(&self, text: Bytes, enter: Bytes, delay: Duration, deadline: Option<Instant>) -> std::io::Result<std_mpsc::Receiver<std::io::Result<()>>> {
            self.lifecycle.invalidate();
            self.queue_input_submission(text, enter, delay, deadline, crate::terminal::lifecycle_binding::InputGuard::Untracked(self.lifecycle.clone()))
        }
        
        pub(crate) fn queue_guarded_submission(&self, text: Bytes, enter: Bytes, delay: Duration, delivery: crate::terminal::lifecycle::Delivery) -> std::io::Result<std_mpsc::Receiver<std::io::Result<()>>> {
            self.queue_input_submission(text, enter, delay, None, crate::terminal::lifecycle_binding::InputGuard::Guarded(delivery))
        }
        
        fn queue_input_submission(&self, text: Bytes, enter: Bytes, delay: Duration, deadline: Option<Instant>, guard: crate::terminal::lifecycle_binding::InputGuard) -> std::io::Result<std_mpsc::Receiver<std::io::Result<()>>> { let accepting = self
            .accepting
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !*accepting {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "pty actor closed",
            ));
        }
        let (reply_tx, reply_rx) = std_mpsc::channel();
        self.data_tx
            .try_send(PtyIoDataCommand::SubmitUserInput {
                guard,
                text,
                enter,
                delay,
                deadline,
                reply: reply_tx,
            })
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "pty input queue is full",
                ),
                mpsc::error::TrySendError::Closed(_) => {
                    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pty actor closed")
                }
            })?;
        Ok(reply_rx) }

        pub(crate) fn write_terminal_response(&self, response: impl FnOnce() -> Option<Bytes>) {
            let _order = self
                .response_order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(bytes) = response().filter(|bytes| !bytes.is_empty()) {
                let _ = self.write_tx.send(PtyIoWriteCommand::Write(bytes));
            }
        }

        pub(crate) fn resize(
            &self,
            rows: u16,
            cols: u16,
            cell_width_px: u32,
            cell_height_px: u32,
            terminal_responses: Vec<Bytes>,
        ) {
            let _ = self
                .control_tx
                .send(PtyIoControlCommand::Resize(PtyResizeRequest {
                    resize: PtyResize {
                        rows,
                        cols,
                        cell_width_px,
                        cell_height_px,
                    },
                    terminal_responses,
                }));
        }

        pub(crate) fn shutdown(&self) {
            if let Ok(mut accepting) = self.accepting.lock() {
                *accepting = false;
            }
            let _ = self.control_tx.send(PtyIoControlCommand::Shutdown);
        }
    }

    pub(crate) struct PtyIoActor;

    impl PtyIoActor {
        pub(crate) fn spawn(config: PtyIoActorConfig) -> std::io::Result<PtyIoActorHandle> {
            let PtyIoActorConfig {
                pane_id,
                master,
                initially_quiesced,
                mut on_read,
                on_reader_exit,
            } = config;

            let mut reader = master
                .try_clone_reader()
                .map_err(|err| std::io::Error::other(err.to_string()))?;
            let mut writer = master
                .take_writer()
                .map_err(|err| std::io::Error::other(err.to_string()))?;
            let (data_tx, mut data_rx) = mpsc::channel::<PtyIoDataCommand>(1024);
            let (control_tx, control_rx) = std_mpsc::channel::<PtyIoControlCommand>();
            let (write_tx, write_rx) = std_mpsc::channel::<PtyIoWriteCommand>();
            let response_order = Arc::new(Mutex::new(()));
            let accepting = Arc::new(Mutex::new(!initially_quiesced));

            std::thread::spawn(move || {
                run_writer(&mut writer, write_rx);
                debug!(pane_id, "windows pty writer thread exiting");
            });

            {
                let write_tx = write_tx.clone();
                let accepting = Arc::clone(&accepting);
                std::thread::spawn(move || {
                    run_input_forwarder(&mut data_rx, write_tx, accepting);
                    debug!(pane_id, "windows pty input thread exiting");
                });
            }

            {
                let write_tx = write_tx.clone();
                let response_order = Arc::clone(&response_order);
                std::thread::spawn(move || {
                    let mut buf = [0u8; 8192];
                    loop {
                        match reader.read(&mut buf) {
                            Ok(0) => break,
                            Ok(n) => {
                                let _order = response_order
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                let result = on_read(&buf[..n]);
                                if result.terminal_responses.into_iter().any(|response| {
                                    write_tx.send(PtyIoWriteCommand::Write(response)).is_err()
                                }) {
                                    break;
                                }
                            }
                            Err(err) => {
                                debug!(pane_id, err = %err, "windows pty reader failed");
                                break;
                            }
                        }
                    }
                    if let Some(on_reader_exit) = on_reader_exit {
                        on_reader_exit();
                    }
                    debug!(pane_id, "windows pty reader thread exiting");
                });
            }

            {
                let write_tx = write_tx.clone();
                std::thread::spawn(move || {
                    for command in control_rx {
                        match command {
                            PtyIoControlCommand::Resize(request) => {
                                let size = request.resize;
                                if let Err(err) = master.resize(PtySize {
                                    rows: size.rows,
                                    cols: size.cols,
                                    pixel_width: size.cell_width_px.min(u16::MAX as u32) as u16,
                                    pixel_height: size.cell_height_px.min(u16::MAX as u32) as u16,
                                }) {
                                    warn!(pane_id, err = %err, "windows pty resize failed");
                                }
                                if request.terminal_responses.into_iter().any(|response| {
                                    write_tx.send(PtyIoWriteCommand::Write(response)).is_err()
                                }) {
                                    break;
                                }
                            }
                            PtyIoControlCommand::Shutdown => break,
                        }
                    }
                    debug!(pane_id, "windows pty control thread exiting");
                });
            }

            Ok(PtyIoActorHandle { lifecycle: Default::default(), data_tx,
            control_tx,
            write_tx,
            response_order,
            accepting, })
        }
    }

    fn run_writer(writer: &mut impl Write, write_rx: std_mpsc::Receiver<PtyIoWriteCommand>) {
        for command in write_rx {
            let result = match command {
                PtyIoWriteCommand::Write(bytes) => write_and_flush(writer, &bytes),
                PtyIoWriteCommand::UserInput(bytes, guard) => guard.write(|| write_and_flush(writer, &bytes)),
                PtyIoWriteCommand::SubmissionPart {
                    guard,
                    bytes,
                    deadline,
                    reply,
                } => {
                    let result = if deadline.is_some_and(|deadline| Instant::now() >= deadline) { Err(input_submission_timed_out()) } else { guard.write(|| write_and_flush(writer, &bytes)) };
                    let failed = result
                        .as_ref()
                        .is_err_and(|err| !matches!(err.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::PermissionDenied));
                    let _ = reply.send(result);
                    if failed {
                        break;
                    }
                    continue;
                }
            };
            if result.is_err() {
                break;
            }
        }
    }

    fn run_input_forwarder(
        data_rx: &mut mpsc::Receiver<PtyIoDataCommand>,
        write_tx: std_mpsc::Sender<PtyIoWriteCommand>,
        accepting: Arc<Mutex<bool>>,
    ) {
        while let Some(command) = data_rx.blocking_recv() {
            match command {
                PtyIoDataCommand::WriteUserInput(bytes, guard) => {
                    if write_tx.send(PtyIoWriteCommand::UserInput(bytes, guard)).is_err() {
                        break;
                    }
                }
                PtyIoDataCommand::SubmitUserInput {
                    guard,
                    text,
                    enter,
                    delay,
                    deadline,
                    reply,
                } => {
                    let result = if deadline.is_some_and(|deadline| {
                        deadline.saturating_duration_since(Instant::now()) <= delay
                    }) {
                        Err(input_submission_timed_out())
                    } else {
                        let text_was_empty = text.is_empty();
                        let text_deadline =
                            deadline.and_then(|deadline| deadline.checked_sub(delay));
                        write_submission_part(&write_tx, text, text_deadline, guard.clone()).and_then(|()| {
                            // Caller timeouts cannot truncate a legacy submission.
                            // Guarded Enter still requires the admitted generation.
                            std::thread::sleep(delay);
                            let accepting = accepting
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            if !*accepting {
                                return Err(pty_actor_closed());
                            }
                            write_submission_part(&write_tx, enter, None, guard.clone()).map_err(|err| {
                                // The text is already in the agent's input box; a rejected Enter must not read as "nothing written".
                                if err.kind() == std::io::ErrorKind::PermissionDenied && !text_was_empty { crate::terminal::lifecycle::partial_delivery() } else { err }
                            })
                        })
                    };
                    if result.is_ok() { guard.complete(); }
                    let failed = result
                        .as_ref()
                        .is_err_and(|err| !matches!(err.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::PermissionDenied));
                    let _ = reply.send(result);
                    if failed {
                        break;
                    }
                }
            }
        }
    }

    fn write_submission_part(write_tx: &std_mpsc::Sender<PtyIoWriteCommand>, bytes: Bytes, deadline: Option<Instant>, guard: crate::terminal::lifecycle_binding::InputGuard) -> std::io::Result<()> {
        if bytes.is_empty() { return Ok(()); }
        let (reply, completion) = std_mpsc::channel();
        write_tx
            .send(PtyIoWriteCommand::SubmissionPart {
                guard,
                bytes,
                deadline,
                reply,
            })
            .map_err(|_| pty_actor_closed())?;
        completion
            .recv()
            .unwrap_or_else(|_| Err(pty_actor_closed()))
    }

    fn pty_actor_closed() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pty actor closed")
    }

    fn input_submission_timed_out() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "agent prompt timed out before input submission",
        )
    }

    fn write_and_flush(writer: &mut impl Write, bytes: &[u8]) -> std::io::Result<()> {
        writer.write_all(bytes)?;
        writer.flush()
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::terminal::lifecycle_binding::{InputGuard, LifecycleBinding};
        struct RecordingWriter {
            writes: Vec<(Vec<u8>, Instant)>,
            flushes: Vec<Instant>,
            fail_after: Option<usize>,
            flushed: std_mpsc::Sender<()>,
        }

        impl Write for RecordingWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.fail_after == Some(self.writes.len()) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "writer closed",
                    ));
                }
                self.writes.push((bytes.to_vec(), Instant::now()));
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                self.flushes.push(Instant::now());
                let _ = self.flushed.send(());
                Ok(())
            }
        }

        fn run_recorded_submission(
            fail_after: Option<usize>,
            delay: Duration,
            deadline: Option<Instant>,
            during_delay: impl FnOnce(&std_mpsc::Sender<PtyIoWriteCommand>, &Arc<Mutex<bool>>),
        ) -> (RecordingWriter, std::io::Result<()>) {
            let (flushed_tx, flushed_rx) = std_mpsc::channel();
            let mut writer = RecordingWriter {
                writes: Vec::new(),
                flushes: Vec::new(),
                fail_after,
                flushed: flushed_tx,
            };
            let (data_tx, mut data_rx) = mpsc::channel(2);
            let (write_tx, write_rx) = std_mpsc::channel();
            let (reply_tx, reply_rx) = std_mpsc::channel();
            let accepting = Arc::new(Mutex::new(true));
            data_tx
                .try_send(PtyIoDataCommand::SubmitUserInput {
                    guard: InputGuard::Untracked(LifecycleBinding::default()),
                    text: Bytes::from_static(b"prompt"),
                    enter: Bytes::from_static(b"\r"),
                    delay,
                    deadline,
                    reply: reply_tx,
                })
                .unwrap();
            data_tx
                .try_send(PtyIoDataCommand::WriteUserInput(
                    Bytes::from_static(b"user"),
                    InputGuard::Untracked(LifecycleBinding::default()),
                ))
                .unwrap();
            let writer_thread = std::thread::spawn(move || {
                run_writer(&mut writer, write_rx);
                writer
            });
            let input_write_tx = write_tx.clone();
            let input_accepting = Arc::clone(&accepting);
            let input_thread = std::thread::spawn(move || {
                run_input_forwarder(&mut data_rx, input_write_tx, input_accepting)
            });
            flushed_rx.recv().expect("prompt was flushed");
            during_delay(&write_tx, &accepting);
            let result = reply_rx.recv().expect("writer reports submission");
            drop(data_tx);
            input_thread.join().expect("input thread joins");
            drop(write_tx);
            (writer_thread.join().expect("writer thread joins"), result)
        }

        #[test]
        fn submission_sequences_user_input_but_allows_terminal_responses() {
            let delay = Duration::from_millis(30);
            let (writer, result) = run_recorded_submission(None, delay, None, |write_tx, _| {
                write_tx
                    .send(PtyIoWriteCommand::Write(Bytes::from_static(b"response")))
                    .unwrap();
            });
            result.expect("submission succeeds");

            assert_eq!(writer.writes[0].0, b"prompt");
            assert_eq!(writer.writes[1].0, b"response");
            assert_eq!(writer.writes[2].0, b"\r");
            assert_eq!(writer.writes[3].0, b"user");
            assert!(writer.writes[2].1.duration_since(writer.flushes[0]) >= delay);
        }

        #[test]
        fn submission_returns_enter_write_failure() {
            let (_writer, result) =
                run_recorded_submission(Some(1), Duration::ZERO, None, |_, _| {});
            let err = result.expect_err("enter failure reaches caller");

            assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
        }

        #[test]
        fn shutdown_during_submission_delay_cancels_enter() {
            let (writer, result) =
                run_recorded_submission(None, Duration::from_millis(30), None, |_, accepting| {
                    *accepting.lock().unwrap() = false;
                });
            let err = result.expect_err("shutdown cancels enter");
            assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
            assert_eq!(
                writer
                    .writes
                    .iter()
                    .map(|write| write.0.as_slice())
                    .collect::<Vec<_>>(),
                vec![b"prompt"]
            );
        }

        #[test]
        fn expired_queued_submission_is_not_written() {
            let (flushed_tx, _flushed_rx) = std_mpsc::channel();
            let mut writer = RecordingWriter {
                writes: Vec::new(),
                flushes: Vec::new(),
                fail_after: None,
                flushed: flushed_tx,
            };
            let (data_tx, mut data_rx) = mpsc::channel(2);
            let (write_tx, write_rx) = std_mpsc::channel();
            let (first_reply_tx, first_reply_rx) = std_mpsc::channel();
            let (expired_reply_tx, expired_reply_rx) = std_mpsc::channel();
            let accepting = Arc::new(Mutex::new(true));
            data_tx
                .try_send(PtyIoDataCommand::SubmitUserInput {
                    guard: InputGuard::Untracked(LifecycleBinding::default()),
                    text: Bytes::from_static(b"first"),
                    enter: Bytes::from_static(b"\r"),
                    delay: Duration::from_millis(30),
                    deadline: None,
                    reply: first_reply_tx,
                })
                .unwrap();
            data_tx
                .try_send(PtyIoDataCommand::SubmitUserInput {
                    guard: InputGuard::Untracked(LifecycleBinding::default()),
                    text: Bytes::from_static(b"expired"),
                    enter: Bytes::from_static(b"\r"),
                    delay: Duration::ZERO,
                    deadline: Some(Instant::now() + Duration::from_millis(10)),
                    reply: expired_reply_tx,
                })
                .unwrap();

            let writer_thread = std::thread::spawn(move || {
                run_writer(&mut writer, write_rx);
                writer
            });
            let input_write_tx = write_tx.clone();
            let input_thread = std::thread::spawn(move || {
                run_input_forwarder(&mut data_rx, input_write_tx, accepting)
            });
            first_reply_rx.recv().unwrap().unwrap();
            let err = expired_reply_rx.recv().unwrap().unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);

            drop(data_tx);
            input_thread.join().unwrap();
            drop(write_tx);
            let writer = writer_thread.join().unwrap();
            assert_eq!(
                writer
                    .writes
                    .iter()
                    .map(|write| write.0.as_slice())
                    .collect::<Vec<_>>(),
                vec![b"first".as_slice(), b"\r".as_slice()]
            );
        }

        #[test]
        fn guarded_writer_rejects_queued_races_without_stopping() {
            use crate::detect::AgentState;
            use crate::terminal::lifecycle::Lifecycle;

            for scenario in ["submit-busy", "abort-settled", "abort-replaced"] {
                let owner = Lifecycle::default();
                let delivery = if scenario == "submit-busy" {
                    owner.observe(AgentState::Idle);
                    owner.submit().unwrap()
                } else {
                    owner.observe(AgentState::Working);
                    owner.abort(&owner.snapshot().1).unwrap()
                };
                let (flushed, _) = std_mpsc::channel();
                let mut writer = RecordingWriter {
                    writes: Vec::new(), flushes: Vec::new(), fail_after: None, flushed,
                };
                let (write_tx, write_rx) = std_mpsc::channel();
                let (reply, completion) = std_mpsc::channel();
                write_tx.send(PtyIoWriteCommand::SubmissionPart {
                    guard: InputGuard::Guarded(delivery),
                    bytes: if scenario == "submit-busy" { Bytes::from_static(b"prompt") } else { Bytes::from_static(b"\x03") },
                    deadline: None,
                    reply,
                }).unwrap();
                owner.observe(AgentState::Idle);
                if scenario != "abort-settled" {
                    owner.observe(AgentState::Working);
                }
                write_tx.send(PtyIoWriteCommand::Write(Bytes::from_static(b"response"))).unwrap();
                drop(write_tx);
                run_writer(&mut writer, write_rx);
                assert_eq!(completion.recv().unwrap().unwrap_err().kind(), std::io::ErrorKind::PermissionDenied, "{scenario}");
                assert_eq!(writer.writes.len(), 1, "{scenario}");
                assert_eq!(writer.writes[0].0, b"response", "{scenario}");
            }
        }

        #[test]
        fn guarded_enter_rechecks_after_text_was_flushed() {
            use crate::detect::AgentState;
            use crate::terminal::lifecycle::Lifecycle;

            let owner = Lifecycle::default();
            owner.observe(AgentState::Idle);
            let delivery = owner.submit().unwrap();
            let (flushed, _) = std_mpsc::channel();
            let mut writer = RecordingWriter {
                writes: Vec::new(), flushes: Vec::new(), fail_after: None, flushed,
            };
            for (index, bytes) in [Bytes::from_static(b"prompt"), Bytes::from_static(b"\r")].into_iter().enumerate() {
                let (write_tx, write_rx) = std_mpsc::channel();
                let (reply, completion) = std_mpsc::channel();
                write_tx.send(PtyIoWriteCommand::SubmissionPart {
                    guard: InputGuard::Guarded(delivery.clone()), bytes, deadline: None, reply,
                }).unwrap();
                drop(write_tx);
                run_writer(&mut writer, write_rx);
                if index == 0 {
                    completion.recv().unwrap().unwrap();
                    owner.observe(AgentState::Working);
                } else {
                    assert_eq!(completion.recv().unwrap().unwrap_err().kind(), std::io::ErrorKind::PermissionDenied);
                }
            }
            assert_eq!(writer.writes.len(), 1);
            assert_eq!(writer.writes[0].0, b"prompt");
        }

        /// Drives the real forwarder + writer: guarded text, delay, guarded Enter.
        fn run_guarded_submission(owner: &crate::terminal::lifecycle::Lifecycle, during_delay: impl FnOnce()) -> (Vec<Vec<u8>>, std::io::Result<()>) {
            let delivery = owner.submit().unwrap();
            let (flushed_tx, flushed_rx) = std_mpsc::channel();
            let mut writer = RecordingWriter { writes: Vec::new(), flushes: Vec::new(), fail_after: None, flushed: flushed_tx };
            let (data_tx, mut data_rx) = mpsc::channel(2);
            let (write_tx, write_rx) = std_mpsc::channel();
            let (reply_tx, reply_rx) = std_mpsc::channel();
            let accepting = Arc::new(Mutex::new(true));
            data_tx.try_send(PtyIoDataCommand::SubmitUserInput {
                guard: InputGuard::Guarded(delivery),
                text: Bytes::from_static(b"prompt"),
                enter: Bytes::from_static(b"\r"),
                delay: Duration::from_millis(150),
                deadline: None,
                reply: reply_tx,
            }).unwrap();
            let writer_thread = std::thread::spawn(move || { run_writer(&mut writer, write_rx); writer });
            let input_write_tx = write_tx.clone();
            let input_thread = std::thread::spawn(move || run_input_forwarder(&mut data_rx, input_write_tx, accepting));
            flushed_rx.recv().expect("prompt was flushed");
            during_delay();
            let result = reply_rx.recv().expect("forwarder reports submission");
            drop(data_tx);
            input_thread.join().unwrap();
            drop(write_tx);
            let writer = writer_thread.join().unwrap();
            (writer.writes.into_iter().map(|w| w.0).collect(), result)
        }

        #[test]
        fn actor_reports_partial_delivery_when_generation_changes_between_text_and_enter() {
            use crate::detect::AgentState;
            use crate::terminal::lifecycle::{Lifecycle, PARTIAL_DELIVERY};
            let owner = Lifecycle::default();
            owner.observe(AgentState::Idle);
            let (writes, result) = run_guarded_submission(&owner, || owner.observe(AgentState::Working));
            let err = result.unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
            assert_eq!(err.to_string(), PARTIAL_DELIVERY);
            assert_eq!(writes, vec![b"prompt".to_vec()], "Enter must not be written after the generation changed");
        }

        #[test]
        fn actor_submission_survives_noop_recompute_and_stale_detection() {
            use crate::detect::AgentState;
            use crate::terminal::lifecycle::Lifecycle;
            let owner = Lifecycle::default();
            owner.observe(AgentState::Idle);
            // Detection still reporting the pre-submit Idle must not cancel the Enter.
            let (writes, result) = run_guarded_submission(&owner, || { owner.observe(AgentState::Idle); owner.observe(AgentState::Idle); });
            result.unwrap();
            assert_eq!(writes, vec![b"prompt".to_vec(), b"\r".to_vec()]);
        }
    }
}

#[cfg(windows)]
pub(crate) use windows::*;
