use std::future::Future;
use std::time::Duration;

use rand::seq::SliceRandom;
use rand::Rng;
use tokio::time;

use crate::sim::state::State;

pub(crate) async fn disk_latency<F: Future>(fut: F) -> F::Output {
    time::sleep(get_delay(Duration::from_millis(30))).await;
    let res = fut.await;
    time::sleep(get_delay(Duration::from_millis(30))).await;
    res
}

pub(crate) async fn network_latency<F: Future>(fut: F) -> F::Output {
    time::sleep(get_delay(Duration::from_millis(500))).await;
    let res = fut.await;
    time::sleep(get_delay(Duration::from_millis(500))).await;
    res
}

fn get_delay(max: Duration) -> Duration {
    State::current(|s| s.rng().gen_range(Duration::from_millis(0)..max))
}

pub(crate) fn shuffle<T>(slice: &mut [T]) {
    State::current(|s| slice.shuffle(&mut s.rng()))
}
