#[derive(Debug, thiserror::Error)]
#[error("simulation failed with seed {seed}: {kind}")]
pub struct SimError {
    seed: u64,
    kind: Kind,
}

#[derive(Debug, thiserror::Error)]
enum Kind {
    #[error("machine {name} failed: {source}")]
    Machine {
        name: String,
        #[source]
        source: Box<dyn std::error::Error>,
    },

    #[error("the root future controlling the simulation failed: {source}")]
    Root {
        #[source]
        source: Box<dyn std::error::Error>,
    },
}

impl SimError {
    pub(crate) fn from_machine(
        name: String,
        seed: u64,
        source: Box<dyn std::error::Error>,
    ) -> Self {
        Self {
            seed,
            kind: Kind::Machine { name, source },
        }
    }

    pub(crate) fn from_root(seed: u64, source: Box<dyn std::error::Error>) -> Self {
        Self {
            seed,
            kind: Kind::Root { source },
        }
    }
}
