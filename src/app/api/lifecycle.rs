use bytes::Bytes;

use crate::api::schema::{AgentLifecycleAction, AgentLifecycleParams, AgentStatus, ResponseResult};
use crate::app::App;
use crate::detect::AgentState;
use super::responses::{encode_error, encode_error_body, encode_success};

fn status(state: AgentState) -> AgentStatus {
    match state {
        AgentState::Idle => AgentStatus::Idle,
        AgentState::Working => AgentStatus::Working,
        AgentState::Blocked => AgentStatus::Blocked,
        _ => AgentStatus::Unknown,
    }
}

impl App {
    pub(crate) fn handle_lifecycle_request(
        &mut self,
        id: String,
        params: AgentLifecycleParams,
        respond_to: std::sync::mpsc::Sender<String>,
    ) {
        let result = self.queue_lifecycle_request(&id, params);
        match result {
            Err(response) => { let _ = respond_to.send(response); }
            Ok((result, None)) => { let _ = respond_to.send(encode_success(id, result)); }
            Ok((result, Some(completion))) => {
                std::thread::spawn(move || {
                    let response = match completion.recv() {
                        Ok(Ok(())) => encode_success(id, result),
                        Ok(Err(err)) if err.kind() == std::io::ErrorKind::PermissionDenied && err.to_string() == crate::terminal::lifecycle::PARTIAL_DELIVERY =>
                            encode_error(id, "partial_delivery", "Prompt text reached the agent but Enter was not sent; the input box may hold unsent text. Not retried"),
                        Ok(Err(err)) if err.kind() == std::io::ErrorKind::PermissionDenied =>
                            encode_error(id, "stale_generation", "Lifecycle changed before delivery; nothing was retried"),
                        Ok(Err(err)) => encode_error(id, "agent_delivery_failed", err.to_string()),
                        Err(_) => encode_error(id, "agent_delivery_failed", "PTY actor closed"),
                    };
                    let _ = respond_to.send(response);
                });
            }
        }
    }

    fn queue_lifecycle_request(
        &mut self,
        id: &str,
        params: AgentLifecycleParams,
    ) -> Result<(ResponseResult, Option<std::sync::mpsc::Receiver<std::io::Result<()>>>), String> {
        let resolved = self.resolve_agent_target(&params.target)
            .map_err(|err| encode_error_body(id.to_owned(), self.agent_target_error_body(err)))?;
        let terminal_id = self.state.workspaces[resolved.ws_idx].terminal_id(resolved.pane_id)
            .ok_or_else(|| encode_error(id.to_owned(), "agent_not_found", "Terminal not found"))?;
        let terminal = self.state.terminals.get(terminal_id)
            .ok_or_else(|| encode_error(id.to_owned(), "agent_not_found", "Terminal not found"))?;
        let owner = terminal.lifecycle.clone();
        let expected_agent = terminal.effective_known_agent()
            .ok_or_else(|| encode_error(id.to_owned(), "unsupported_lifecycle", "No authoritative agent identity"))?;
        if terminal.managed_agent_launch_pending() {
            return Err(encode_error(id.to_owned(), "agent_not_ready", "Agent launch is still pending"));
        }
        let runtime = self.lookup_runtime_sender(resolved.ws_idx, resolved.pane_id)
            .ok_or_else(|| encode_error(id.to_owned(), "agent_not_found", "Runtime not found"))?;
        if !runtime.lifecycle_matches(&owner) || !super::super::agents::runtime_hosts_agent(runtime, expected_agent) {
            return Err(encode_error(id.to_owned(), "unsupported_lifecycle", "Runtime lifecycle owner is unavailable"));
        }
        let (snapshot, completion) = match params.action {
            AgentLifecycleAction::Snapshot => (owner.snapshot(), None),
            AgentLifecycleAction::Submit { text } => {
                if text.trim().is_empty() {
                    return Err(encode_error(id.to_owned(), "empty_agent_prompt", "Prompt must not be empty"));
                }
                let delivery = owner.submit().map_err(|code| encode_error(id.to_owned(), code, "Agent is not idle"))?;
                let delay = super::agents::agent_prompt_submit_delay(expected_agent, text.len());
                let (mut text, enter) = crate::app::api_helpers::encode_api_submission_parts(runtime, &text);
                                if expected_agent == crate::detect::Agent::GithubCopilot {
                                    let mut focus = crate::ghostty::encode_focus(crate::ghostty::FocusEvent::Gained)
                                        .map_err(|err| {
                                            owner.invalidate();
                                            encode_error(id.to_owned(), "agent_delivery_failed", err.to_string())
                                        })?;
                                    focus.append(&mut text);
                                    text = focus;
                                }
                (delivery.snapshot(), Some(runtime.queue_guarded_submission(
                    Bytes::from(text), Bytes::from(enter), delay, delivery,
                ).map_err(|err| {
                    owner.invalidate();
                    encode_error(id.to_owned(), "agent_delivery_failed", err.to_string())
                })?))
            }
            AgentLifecycleAction::Abort { generation } => {
                let delivery = owner.abort(&generation)
                    .map_err(|code| encode_error(id.to_owned(), code, "Observed generation is no longer active"))?;
                let keys = super::super::api_helpers::encode_api_keys(runtime, &["ctrl+c".to_owned()])
                    .map_err(|_| encode_error(id.to_owned(), "unsupported_lifecycle", "Cannot encode interrupt"))?;
                (delivery.snapshot(), Some(runtime.queue_guarded_submission(
                    Bytes::from(keys.into_iter().flatten().collect::<Vec<u8>>()), Bytes::new(),
                    std::time::Duration::ZERO, delivery,
                ).map_err(|err| encode_error(id.to_owned(), "agent_delivery_failed", err.to_string()))?))
            }
        };
        let (state, generation) = snapshot;
        Ok((ResponseResult::AgentLifecycle {
            status: status(state), generation, delivered: completion.is_some(),
        }, completion))
    }
}
