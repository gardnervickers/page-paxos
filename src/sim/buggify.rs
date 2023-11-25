use std::future::Future;
use std::time::Duration;

#[cfg(test)]
use rand::seq::SliceRandom;
use rand::Rng;
#[cfg(test)]
use tokio::time;

use crate::sim::state::State;

pub(crate) async fn disk_latency<F: Future>(fut: F) -> F::Output {
    #[cfg(test)]
    {
        time::sleep(get_delay(Duration::from_millis(10))).await;
        let res = fut.await;
        time::sleep(get_delay(Duration::from_millis(10))).await;
        res
    }
    #[cfg(not(test))]
    {
        fut.await
    }
}

pub(crate) async fn network_latency<F: Future>(fut: F) -> F::Output {
    #[cfg(test)]
    {
        time::sleep(get_delay(Duration::from_millis(500))).await;
        let res = fut.await;
        time::sleep(get_delay(Duration::from_millis(500))).await;
        res
    }
    #[cfg(not(test))]
    {
        fut.await
    }
}

fn get_delay(max: Duration) -> Duration {
    State::current(|s| s.rng().gen_range(Duration::from_millis(0)..max))
}

pub(crate) fn shuffle<T>(slice: &mut [T]) {
    #[cfg(test)]
    {
        State::current(|s| slice.shuffle(&mut s.rng()));
    }
}

pub(crate) fn acceptor_flake() -> bool {
    State::current(|s| s.rng().gen_bool(0.05))
}
