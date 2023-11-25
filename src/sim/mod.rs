//! Simulator for tests.
//!
//! TODO: This should probably be split out into a seperate crate.
pub(crate) mod buggify;
mod error;
mod rng;
mod state;
mod trial;

use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub use error::SimError;
use rand::Rng;
use tracing::Instrument;

pub struct Sim {
    runtime: tokio::runtime::Runtime,
    state: state::State,
}

impl Sim {
    pub fn new() -> Self {
        Self::new_with_seed(rand::thread_rng().gen())
    }

    pub fn new_with_seed(seed: u64) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        runtime.block_on(async {
            // Round now to the nearest millisecond.
            tokio::time::sleep(Duration::from_millis(1)).await;
        });
        Self {
            runtime,
            state: state::State::new(seed),
        }
    }

    /// Adds a machine to the simulation.
    pub fn add_machine<S, F>(&mut self, name: S, future: F)
    where
        F: Future<Output = Result<(), Box<dyn std::error::Error>>> + 'static,
        S: AsRef<str>,
    {
        let name = name.as_ref().to_owned();

        let mut trial = self.state.trial();
        trial.add_machine(name, future);
    }

    #[allow(clippy::await_holding_lock)]
    pub fn block_on<F, U>(self, future: F) -> Result<U, SimError>
    where
        F: Future<Output = Result<U, Box<dyn std::error::Error>>>,
    {
        let seed = self.state.seed();
        let runtime = self.runtime;
        let state = self.state;
        // Catch panic and convert to error.
        let panic = state.enter(|| {
            panic::catch_unwind(panic::AssertUnwindSafe(|| {
                runtime.block_on(async move {
                    SimFuture::new(future)
                        .instrument(tracing::info_span!("sim"))
                        .await
                })
            }))
        });
        match panic {
            Ok(result) => result,
            Err(panic) => {
                eprintln!("panic during simulation seed={}", seed);
                panic::resume_unwind(panic);
            }
        }
    }

    pub fn handle(&self) -> Handle {
        Handle {}
    }
}

impl Default for Sim {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct Handle;

impl Handle {
    pub fn rng(&self) -> impl rand::RngCore {
        state::State::current(|s| s.rng().clone())
    }

    pub async fn wait_machines(&self) {
        state::State::current(|s| s.notify_all_machines_complete().clone())
            .notified()
            .await;
    }

    pub fn current() -> Self {
        Self {}
    }
}

pin_project_lite::pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    struct SimFuture<F> {
        #[pin]
        future: F,
    }
}

impl<F> SimFuture<F> {
    fn new(future: F) -> Self {
        Self { future }
    }
}

impl<F, T> Future for SimFuture<F>
where
    F: Future<Output = Result<T, Box<dyn std::error::Error>>>,
{
    type Output = Result<T, SimError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // First try to poll the trial.
        let seed = state::State::current(|s| s.seed());
        match state::State::current(|s| s.trial().poll_all(cx)) {
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => {
                // All machines completed, trigger notification.
                state::State::current(|s| s.notify_all_machines_complete().notify_waiters());
            }
            Poll::Pending => (),
        };
        let this = self.project();
        this.future
            .poll(cx)
            .map_err(|err| SimError::from_root(seed, err))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    use tracing_subscriber::util::SubscriberInitExt;

    use super::*;

    /// Test that time advances faster than wallclock time.
    #[test]
    fn time_advances() -> Result<(), Box<dyn std::error::Error>> {
        let _g = tracing_subscriber::fmt()
            .with_test_writer()
            .without_time()
            .finish()
            .set_default();
        let mut sim = Sim::new_with_seed(0);
        let handle = sim.handle();
        let real_now = std::time::Instant::now();

        let completion_order = Rc::new(RefCell::new(vec![]));

        let co = Rc::clone(&completion_order);
        sim.add_machine("foo", async move {
            tokio::time::sleep(Duration::from_millis(10000)).await;
            tracing::info!("hello from foo");
            co.borrow_mut().push("foo");
            Ok(())
        });

        let co = Rc::clone(&completion_order);
        sim.add_machine("bar", async move {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            tracing::info!("hello from bar");
            co.borrow_mut().push("bar");
            Ok(())
        });

        sim.block_on(async {
            let sim_time = tokio::time::Instant::now();
            handle.wait_machines().await;
            let sim_elapsed = sim_time.elapsed();

            let real_elapsed = real_now.elapsed();
            assert!(
                sim_elapsed > real_elapsed,
                "simulated time should advance faster than real time"
            );

            assert_eq!(
                completion_order.borrow().as_slice(),
                &["bar", "foo"],
                "bar should always complete before foo"
            );
            Ok(())
        })?;

        Ok(())
    }
}
