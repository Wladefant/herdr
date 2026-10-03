//! Admission and delivery share this lock. A snapshot never grants permission to write.
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::detect::AgentState;

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct State {
    status: AgentState,
    /// Last status reported by detection; submit/abort move `status` without touching it.
    observed: Option<AgentState>,
    generation: u64,
    reserved: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct Lifecycle(Arc<Mutex<State>>);

#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    owner: Lifecycle,
    generation: u64,
    status: AgentState,
    abort: bool,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(State {
            status: AgentState::Unknown,
            observed: None,
            generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            reserved: false,
        })))
    }
}

fn active(status: AgentState) -> bool {
    matches!(status, AgentState::Working | AgentState::Blocked)
}

fn stale() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::PermissionDenied, "stale_generation")
}

/// Input already reached the agent but the guard rejected the rest of the submission.
pub(crate) const PARTIAL_DELIVERY: &str = "partial_delivery";

pub(crate) fn partial_delivery() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::PermissionDenied, PARTIAL_DELIVERY)
}

impl Lifecycle {
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn write_untracked<T>(&self, write: impl FnOnce() -> T) -> T {
        let mut state = self.lock();
        state.generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
        state.status = AgentState::Unknown;
        state.observed = None;
        state.reserved = false;
        write()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(|poisoned| {
            let mut state = poisoned.into_inner();
            state.generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
            state.status = AgentState::Unknown;
            state.observed = None;
            state.reserved = false;
            state
        })
    }

    /// Only a real change in the detected status invalidates outstanding operations; a no-op
    /// recompute, or detection still reporting the pre-submit status, must not.
    pub(crate) fn observe(&self, status: AgentState) {
        let mut state = self.lock();
        if state.observed == Some(status) {
            return;
        }
        state.observed = Some(status);
        state.generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
        state.status = status;
        state.reserved = false;
    }

    /// Untracked input or a replaced occupant invalidates every outstanding operation.
    pub(crate) fn invalidate(&self) {
        let mut state = self.lock();
        state.generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
        state.status = AgentState::Unknown;
        state.observed = None;
        state.reserved = false;
    }

    pub(crate) fn snapshot(&self) -> (AgentState, String) {
        let state = self.lock();
        (state.status, self.token(state.generation))
    }

    fn token(&self, generation: u64) -> String {
        
        static EPOCH: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| { let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        format!("{}-{nanos}", std::process::id()) });
                let epoch = &*EPOCH;
        format!("{epoch}-{generation}")
    }

    pub(crate) fn submit(&self) -> Result<Delivery, &'static str> {
        let mut state = self.lock();
        if state.status != AgentState::Idle || state.reserved {
            return Err("agent_busy");
        }
        state.generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
        state.status = AgentState::Working;
        state.reserved = true;
        Ok(Delivery { owner: self.clone(), generation: state.generation, status: state.status, abort: false })
    }

    pub(crate) fn abort(&self, generation: &str) -> Result<Delivery, &'static str> {
        let state = self.lock();
        if !active(state.status) || self.token(state.generation) != generation {
            return Err("stale_generation");
        }
        Ok(Delivery { owner: self.clone(), generation: state.generation, status: state.status, abort: true })
    }
}

impl Delivery {
    pub(crate) fn snapshot(&self) -> (AgentState, String) {
        (self.status, self.owner.token(self.generation))
    }

    /// The owner remains locked through the actual PTY write, not just its enqueue.
    pub(crate) fn write<T>(&self, write: impl FnOnce() -> std::io::Result<T>) -> std::io::Result<T> {
        let state = self.owner.lock();
        if state.generation != self.generation || !active(state.status) {
            return Err(stale());
        }
        write()
    }

    pub(crate) fn complete(&self) {
        if self.abort {
            let mut state = self.owner.lock();
            if state.generation == self.generation {
                state.generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
                state.status = AgentState::Unknown;
                state.reserved = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchanged_observation_keeps_generation_and_reservation() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Working);
        let token = owner.snapshot().1;
        owner.observe(AgentState::Working);
        assert_eq!(owner.snapshot().1, token);
        assert!(owner.abort(&token).is_ok());
        let idle = Lifecycle::default();
        idle.observe(AgentState::Idle);
        let _delivery = idle.submit().unwrap();
        idle.observe(AgentState::Idle);
        assert!(idle.submit().is_err(), "stale idle detection must not reopen a reserved turn");
    }

    #[test]
    fn idle_snapshot_cannot_admit_after_competing_start() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Idle);
        assert_eq!(owner.snapshot().0, AgentState::Idle);
        owner.observe(AgentState::Working);
        assert_eq!(owner.submit().unwrap_err(), "agent_busy");
    }

    #[test]
    fn competing_submit_reserves_exactly_one_turn() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Idle);
        let first = owner.submit().unwrap();
        assert_eq!(owner.submit().unwrap_err(), "agent_busy");
        let mut writes = 0;
        first.write(|| { writes += 1; Ok(()) }).unwrap();
        assert_eq!(writes, 1);
    }

    #[test]
    fn queued_submission_is_cancelled_before_delivery() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Idle);
        let delivery = owner.submit().unwrap();
        owner.invalidate();
        let mut writes = 0;
        assert!(delivery.write(|| { writes += 1; Ok(()) }).is_err());
        assert_eq!(writes, 0);
    }

    #[test]
    fn settlement_rejects_observed_abort_without_writing() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Working);
        let generation = owner.snapshot().1;
        let delivery = owner.abort(&generation).unwrap();
        owner.observe(AgentState::Idle);
        let mut writes = 0;
        assert!(owner.abort(&generation).is_err());
        assert!(delivery.write(|| { writes += 1; Ok(()) }).is_err());
        assert_eq!(writes, 0);
    }

    #[test]
    fn subsequent_generation_rejects_queued_stale_abort() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Working);
        let generation = owner.snapshot().1;
        let delivery = owner.abort(&generation).unwrap();
        owner.observe(AgentState::Idle);
        owner.observe(AgentState::Working);
        assert_ne!(owner.snapshot().1, generation);
        assert!(owner.abort(&generation).is_err());
        assert!(delivery.write::<()>(|| panic!("stale abort reached PTY")).is_err());
    }

    #[test]
    fn matching_abort_writes_once_and_invalidates_duplicate() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Working);
        owner.observe(AgentState::Blocked);
        let generation = owner.snapshot().1;
        let delivery = owner.abort(&generation).unwrap();
        delivery.write(|| Ok(())).unwrap();
        delivery.complete();
        assert!(owner.abort(&generation).is_err());
    }

    #[test]
    fn concurrent_submit_clients_admit_exactly_one() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Idle);
        let barrier = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            let clients: Vec<_> = (0..2).map(|_| scope.spawn(|| {
                barrier.wait();
                owner.submit()
            })).collect();
            barrier.wait();
            let mut admissions = 0;
            for client in clients {
                if let Ok(delivery) = client.join().unwrap() {
                    delivery.write(|| Ok(())).unwrap();
                    admissions += 1;
                }
            }
            assert_eq!(admissions, 1);
        });
    }

    #[test]
    fn delivery_holds_owner_lock_through_physical_write() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Idle);
        let delivery = owner.submit().unwrap();
        delivery.write(|| {
            std::thread::scope(|scope| {
                scope.spawn(|| assert!(owner.0.try_lock().is_err())).join().unwrap();
            });
            Ok(())
        }).unwrap();
    }

    #[test]
    fn completed_abort_keeps_its_admission_receipt() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Working);
        let snapshot = owner.snapshot();
        let delivery = owner.abort(&snapshot.1).unwrap();
        delivery.write(|| Ok(())).unwrap();
        delivery.complete();
        assert_ne!(owner.snapshot().1, snapshot.1);
        assert_eq!(delivery.snapshot(), snapshot);
    }

    #[test]
    fn poisoned_owner_rejects_further_delivery() {
        let owner = Lifecycle::default();
        owner.observe(AgentState::Idle);
        let delivery = owner.submit().unwrap();
        let _ = std::panic::catch_unwind(|| {
            let _ = delivery.write::<()>(|| panic!("failed physical writer"));
        });
        owner.observe(AgentState::Idle);
        assert!(owner.submit().is_err());
        assert!(delivery.write::<()>(|| panic!("poisoned writer was called")).is_err());
    }
}
