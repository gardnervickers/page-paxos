use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::sim::rng;
use crate::sim::SimError;

use rand::seq::SliceRandom;

type SimResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// [`Machine`] represents a single node in the simulation.
///
/// Each machine has a future which represents the main entrypoint for the machine.
/// Once the future completes, the machine is considered to be terminated.
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct Machine {
    /// The main entrypoint for the machine.
    future: Pin<Box<dyn Future<Output = SimResult>>>,

    /// Background tasks spawned by the machine.
    localset: tokio::task::LocalSet,
}

impl Future for Machine {
    type Output = SimResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // We always poll the localset first, as it may trigger completion of the future.
        let _ = Pin::new(&mut self.localset).poll(cx);
        Pin::new(&mut self.future).poll(cx)
    }
}

/// [`Trial`] captures the state for a single simulation trial.
pub(crate) struct Trial {
    /// The seed used for this simulation.
    seed: u64,

    /// RNG used for this simulation. This is seeded with the `seed` field.
    rng: rng::SimRng,

    /// Mapping from a machine name to the machine itself.
    machines: HashMap<String, Machine>,
}

impl Trial {
    /// Create a new [`Trial`] with the given seed.
    pub(crate) fn new(seed: u64, rng: rng::SimRng) -> Self {
        Self {
            seed,
            machines: HashMap::new(),
            rng,
        }
    }

    pub(crate) fn add_machine<F>(&mut self, name: String, future: F)
    where
        F: Future<Output = SimResult> + 'static,
    {
        let machine = Machine {
            future: Box::pin(future),
            localset: tokio::task::LocalSet::new(),
        };
        if self.machines.contains_key(&name) {
            panic!("machine with name {} already exists", name);
        }
        self.machines.insert(name, machine);
    }

    /// Poll all machines in the [`Trial`]. As machines exit, they will
    /// be removed from this trial.
    ///
    /// Returns `Poll::Ready` if all machines have exited, `Poll::Pending` otherwise.
    pub(crate) fn poll_all(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SimError>> {
        let mut keys = self.machines.keys().cloned().collect::<Vec<_>>();
        // Randomize the poll order for machines.
        keys.shuffle(&mut self.rng);
        for name in keys {
            let machine = self.machines.get_mut(&name).unwrap();
            match Pin::new(machine).poll(cx) {
                Poll::Ready(Ok(())) => {
                    log::debug!("machine.exit");
                    self.machines.remove(&name);
                }
                Poll::Ready(Err(err)) => {
                    self.machines.remove(&name);
                    return Poll::Ready(Err(SimError::from_machine(name, self.seed, err)));
                }
                Poll::Pending => {
                    log::debug!("machine.pending")
                }
            }
        }
        if self.machines.is_empty() {
            log::debug!("machines.empty");
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
