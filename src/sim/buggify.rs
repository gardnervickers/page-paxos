use std::cell::{Cell, RefCell};
use std::future::Future;
use std::ops::Range;
use std::time::Duration;

use rand::seq::SliceRandom;
use rand::Rng;

use super::rng;
use super::state::State;

pub(crate) struct Buggify {
    disk_latency_range: RefCell<Range<Duration>>,
    network_latency_range: RefCell<Range<Duration>>,
    acceptor_flake_chance: Cell<f64>,
}

impl Buggify {
    pub(crate) fn new() -> Self {
        Self {
            disk_latency_range: RefCell::new(Duration::from_millis(0)..Duration::from_millis(50)),
            network_latency_range: RefCell::new(
                Duration::from_millis(0)..Duration::from_millis(10),
            ),
            acceptor_flake_chance: Cell::new(0.0),
        }
    }

    pub(crate) fn set_disk_latency(&self, range: Range<Duration>) {
        *self.disk_latency_range.borrow_mut() = range;
    }

    pub fn set_network_latency(&self, range: Range<Duration>) {
        *self.network_latency_range.borrow_mut() = range;
    }

    pub(crate) fn set_acceptor_flake_chance(&self, chance: f64) {
        self.acceptor_flake_chance.set(chance);
    }

    fn disk_lat(&self, rng: &mut rng::SimRng) -> Duration {
        rng.gen_range(self.disk_latency_range.borrow().clone())
    }

    fn network_lat(&self, rng: &mut rng::SimRng) -> Duration {
        rng.gen_range(self.network_latency_range.borrow().clone())
    }
}

pub(crate) async fn disk_latency<F: Future>(fut: F) -> F::Output {
    if let Some((before_lat, end_lat)) = State::try_current(|s| {
        let bugs = s.bugs();
        let l1 = bugs.disk_lat(&mut s.rng());
        let l2 = bugs.disk_lat(&mut s.rng());
        (l1, l2)
    }) {
        tokio::time::sleep(before_lat).await;
        let res = fut.await;
        tokio::time::sleep(end_lat).await;
        res
    } else {
        fut.await
    }
}

pub(crate) async fn network_latency<F: Future>(fut: F) -> F::Output {
    if let Some((before_lat, end_lat)) = State::try_current(|s| {
        let bugs = s.bugs();
        let l1 = bugs.network_lat(&mut s.rng());
        let l2 = bugs.network_lat(&mut s.rng());
        (l1, l2)
    }) {
        tokio::time::sleep(before_lat).await;
        let res = fut.await;
        tokio::time::sleep(end_lat).await;
        res
    } else {
        fut.await
    }
}

pub(crate) fn acceptor_flake() -> bool {
    State::try_current(|s| {
        let flake_chance = s.bugs().acceptor_flake_chance.get();
        let mut rng = s.rng();
        rng.gen_bool(flake_chance)
    })
    .unwrap_or(false)
}

pub(crate) fn shuffle<T>(slice: &mut [T]) {
    State::try_current(|s| {
        let mut rng = s.rng();
        slice.shuffle(&mut rng);
    });
}
