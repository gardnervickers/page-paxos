use std::sync::{Arc, Mutex};

use rand::SeedableRng;

#[derive(Clone)]
pub(crate) struct SimRng {
    rng: Arc<Mutex<dyn rand::RngCore + Send>>,
    seed: u64,
}

impl std::fmt::Debug for SimRng {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimRng").field("seed", &self.seed).finish()
    }
}

impl SimRng {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            rng: Arc::new(Mutex::new(rand::rngs::SmallRng::seed_from_u64(seed))),
            seed,
        }
    }
}

impl rand::RngCore for SimRng {
    fn next_u32(&mut self) -> u32 {
        self.rng.lock().unwrap().next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.rng.lock().unwrap().next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.rng.lock().unwrap().fill_bytes(dest)
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.rng.lock().unwrap().try_fill_bytes(dest)
    }
}
