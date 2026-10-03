use std::sync::{Arc, OnceLock};
use super::lifecycle::{Delivery, Lifecycle};

/// Bound when the server pairs a terminal's state and PTY runtime.
#[derive(Debug, Clone, Default)]
pub(crate) struct LifecycleBinding(Arc<OnceLock<Lifecycle>>);

impl LifecycleBinding {
    pub(crate) fn bind(&self, owner: &Lifecycle) {
        let _ = self.0.set(owner.clone());
    }

    pub(crate) fn matches(&self, owner: &Lifecycle) -> bool {
        self.0.get().is_some_and(|bound| bound.same_owner(owner))
    }

    pub(crate) fn invalidate(&self) {
        if let Some(owner) = self.0.get() { owner.invalidate(); }
    }

    pub(crate) fn write<T>(&self, write: impl FnOnce() -> T) -> T {
        match self.0.get() {
            Some(owner) => owner.write_untracked(write),
            None => write(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum InputGuard {
    Untracked(LifecycleBinding),
    Guarded(Delivery),
}

impl InputGuard {
    pub(crate) fn complete(&self) {
        if let Self::Guarded(delivery) = self { delivery.complete(); }
    }

    pub(crate) fn write<T>(&self, write: impl FnOnce() -> std::io::Result<T>) -> std::io::Result<T> {
        match self {
            Self::Untracked(binding) => binding.write(write),
            Self::Guarded(delivery) => delivery.write(write),
        }
    }
}
