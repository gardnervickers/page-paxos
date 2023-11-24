//! Thread-local context for the simulation.
//!
//! Simulation runs execute on a single thread. To support this,
//! we store the simulation state in a thread-local variable.
//!
//! The main benefit is that we don't need to worry about Sync/Send
//! assumptions leaking into the simulation code.

use std::cell::{RefCell, RefMut};
use std::rc::Rc;

use scoped_tls::scoped_thread_local;
use tokio::sync::Notify;

use crate::sim::{rng, trial};

scoped_thread_local! {
    static CURRENT: State
}

pub(crate) struct State {
    trial: RefCell<trial::Trial>,
    rng: rng::SimRng,
    seed: u64,
    notify_all_machines_complete: Rc<Notify>,
}

impl State {
    pub(crate) fn new(seed: u64) -> Self {
        let rng = rng::SimRng::new(seed);
        Self {
            trial: RefCell::new(trial::Trial::new(seed, rng.clone())),
            notify_all_machines_complete: Rc::new(Notify::new()),
            rng,
            seed,
        }
    }

    pub(crate) fn enter<F, U>(&self, f: F) -> U
    where
        F: FnOnce() -> U,
    {
        CURRENT.set(self, f)
    }

    pub(crate) fn current<F, U>(f: F) -> U
    where
        F: FnOnce(&Self) -> U,
    {
        CURRENT.with(|c| f(c))
    }

    pub(crate) fn rng(&self) -> rng::SimRng {
        self.rng.clone()
    }

    pub(crate) fn seed(&self) -> u64 {
        self.seed
    }

    pub(crate) fn trial(&self) -> RefMut<'_, trial::Trial> {
        self.trial.borrow_mut()
    }

    pub(crate) fn notify_all_machines_complete(&self) -> Rc<Notify> {
        Rc::clone(&self.notify_all_machines_complete)
    }
}
