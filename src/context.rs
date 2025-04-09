// This is free and unencumbered software released into the public domain.

use std::sync::Arc;

use crossbeam::atomic::AtomicCell;

pub fn new_cancel_context() -> (Context, Canceller) {
    let val = Arc::new(AtomicCell::new(false));

    (
        Context {
            cancelled: val.clone(),
        },
        Canceller {
            cancelled: val.clone(),
        },
    )
}

#[derive(Clone)]
pub struct Context {
    cancelled: Arc<AtomicCell<bool>>,
}

impl Context {
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load()
    }
}

#[derive(Clone)]
pub struct Canceller {
    cancelled: Arc<AtomicCell<bool>>,
}

impl Canceller {
    #[inline]
    pub fn cancel(&self) {
        self.cancelled.store(true);
    }
}
