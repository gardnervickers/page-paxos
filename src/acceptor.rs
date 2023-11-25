use std::io;
use std::rc::Rc;

use crate::{Ballot, Slot};

pub(crate) trait Acceptor {
    type Error: std::error::Error + Send + Sync + 'static;

    fn num_slots(&self) -> usize;

    async fn prepare(&self, slot: Slot, ballot: Ballot) -> Result<PrepareResult, Self::Error>;

    async fn accept(&self, slot: Slot, value: VersionedValue) -> Result<AcceptResult, Self::Error>;
}

impl<T> Acceptor for Rc<T>
where
    T: Acceptor,
{
    type Error = T::Error;

    fn num_slots(&self) -> usize {
        (**self).num_slots()
    }

    async fn prepare(&self, slot: Slot, ballot: Ballot) -> Result<PrepareResult, Self::Error> {
        (**self).prepare(slot, ballot).await
    }

    async fn accept(&self, slot: Slot, value: VersionedValue) -> Result<AcceptResult, Self::Error> {
        (**self).accept(slot, value).await
    }
}

#[derive(Debug)]
struct AcceptorImpl<R> {
    registers: R,
}

impl<R> AcceptorImpl<R> {
    pub(crate) fn new(registers: R) -> Self {
        Self { registers }
    }
}

impl<R> Acceptor for AcceptorImpl<R>
where
    R: Registers,
{
    type Error = Error;

    fn num_slots(&self) -> usize {
        self.registers.num_slots()
    }

    async fn prepare(&self, slot: Slot, ballot: Ballot) -> Result<PrepareResult, Error> {
        // 1. Return a conflict if it already saw a greater ballot number.

        self.registers
            .update(slot, |register| {
                if ballot <= register.promise {
                    return Ok(PrepareResult::Conflict(register.promise));
                }
                if ballot <= register.ballot {
                    return Ok(PrepareResult::Conflict(register.ballot));
                }
                // 2. Set the promise to the ballot number.
                register.promise = ballot;

                let value = VersionedValue {
                    ballot: register.ballot,
                    value: register.value.clone(),
                };
                Ok(PrepareResult::Prepared(value))
            })
            .await
            .expect("handle IO error")
    }

    async fn accept(&self, slot: Slot, value: VersionedValue) -> Result<AcceptResult, Error> {
        self.registers
            .update(slot, |register| {
                if value.ballot() < register.promise {
                    return Ok(AcceptResult::Conflict {
                        ballot: register.promise,
                    });
                }
                if value.ballot() <= register.ballot {
                    return Ok(AcceptResult::Conflict {
                        ballot: register.ballot,
                    });
                }
                register.promise = value.ballot();
                register.ballot = value.ballot();
                register.value = value.into_value();
                Ok(AcceptResult::Accepted)
            })
            .await
            .expect("TODO: Handle IO errors")
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PrepareResult {
    Prepared(VersionedValue),
    Conflict(Ballot),
}

impl PrepareResult {
    pub(crate) fn conflict(&self) -> Option<Ballot> {
        match self {
            Self::Prepared(_) => None,
            Self::Conflict(ballot) => Some(*ballot),
        }
    }

    pub(crate) fn value(&self) -> Option<&VersionedValue> {
        match self {
            Self::Prepared(value) => Some(value),
            Self::Conflict(_) => None,
        }
    }
    pub(crate) fn into_value(self) -> Option<VersionedValue> {
        match self {
            Self::Prepared(value) => Some(value),
            Self::Conflict(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionedValue {
    ballot: Ballot,
    value: Option<Vec<u8>>,
}

impl VersionedValue {
    pub(crate) fn new(ballot: Ballot, value: Option<Vec<u8>>) -> Self {
        Self { ballot, value }
    }
    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub(crate) fn into_value(self) -> Option<Vec<u8>> {
        self.value
    }

    pub(crate) fn ballot(&self) -> Ballot {
        self.ballot
    }
}

impl std::cmp::Ord for VersionedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ballot.cmp(&other.ballot)
    }
}
impl std::cmp::PartialOrd for VersionedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AcceptResult {
    Conflict { ballot: Ballot },
    Accepted,
}
impl AcceptResult {
    pub(crate) fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted)
    }
}

#[derive(Debug, Clone)]
struct Register {
    promise: Ballot,
    ballot: Ballot,
    value: Option<Vec<u8>>,
}

impl Default for Register {
    fn default() -> Self {
        Self {
            promise: Ballot::UNKNOWN,
            ballot: Ballot::UNKNOWN,
            value: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
enum Error {}

trait Registers {
    fn num_slots(&self) -> usize;

    async fn update<U>(&self, slot: Slot, f: impl FnOnce(&mut Register) -> U) -> io::Result<U>;
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;

    use tokio::sync::Mutex;
    use tokio::time;

    use crate::proposer::Proposer;
    use crate::sim::{self, buggify};
    use crate::ProposerId;

    use super::*;

    #[derive(Debug)]
    struct InMemoryRegisters {
        num_slots: usize,
        registers: Mutex<Vec<Register>>,
    }

    impl InMemoryRegisters {
        fn new(num_slots: usize) -> Self {
            Self {
                num_slots,
                registers: Mutex::new(vec![Register::default(); num_slots]),
            }
        }
    }

    impl Registers for InMemoryRegisters {
        fn num_slots(&self) -> usize {
            self.num_slots
        }

        async fn update<U>(&self, slot: Slot, f: impl FnOnce(&mut Register) -> U) -> io::Result<U> {
            buggify::disk_latency(async move {
                let mut lock = self.registers.lock().await;
                let reg = lock.get_mut(slot.0 as usize).unwrap();
                let ret = f(reg);
                Ok(ret)
            })
            .await
        }
    }

    fn init() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
    }

    #[derive(Clone)]
    struct TestAcceptors {
        acceptors: Vec<Rc<AcceptorImpl<InMemoryRegisters>>>,
    }

    impl TestAcceptors {
        fn new(num_acceptors: usize, num_slots: usize) -> Self {
            let acceptors = (0..num_acceptors)
                .map(|_| AcceptorImpl::new(InMemoryRegisters::new(num_slots)))
                .map(Rc::new)
                .collect();
            Self { acceptors }
        }

        fn proposer(&self, id: ProposerId) -> Proposer<Rc<AcceptorImpl<InMemoryRegisters>>> {
            Proposer::new(id, self.acceptors.clone())
        }
    }

    async fn inc_and_get<A>(proposer: &mut Proposer<A>, stats: &TestStats) -> u64
    where
        A: Acceptor + std::fmt::Debug,
    {
        let pid = proposer.id();
        loop {
            if let Ok(res) = proposer.cas(Slot(0), do_inc).await {
                stats.record_success();
                return res
                    .value()
                    .map(|v| u64::from_be_bytes(v.try_into().unwrap()))
                    .unwrap_or(0);
            }
            stats.record_conflict();
            time::sleep(Duration::from_millis(100)).await;
            log::info!("{:?} conflict detected, retrying", pid);
        }
    }

    async fn get<A>(proposer: &mut Proposer<A>) -> u64
    where
        A: Acceptor,
    {
        loop {
            if let Ok(res) = proposer.cas(Slot(0), |v| v.map(|s| s.to_owned())).await {
                return res
                    .value()
                    .map(|v| u64::from_be_bytes(v.try_into().unwrap()))
                    .unwrap_or(0);
            }
        }
    }

    fn do_inc(old: Option<&[u8]>) -> Option<Vec<u8>> {
        match old {
            Some(old_val) => {
                let existing = u64::from_be_bytes(old_val.try_into().unwrap());
                let new = existing + 1;
                Some(new.to_be_bytes().to_vec())
            }
            None => Some(1u64.to_be_bytes().to_vec()),
        }
    }

    struct TestStats {
        num_conflicts: Cell<usize>,
        num_success: Cell<usize>,
    }

    impl TestStats {
        fn new() -> Self {
            Self {
                num_conflicts: Cell::new(0),
                num_success: Cell::new(0),
            }
        }

        fn record_conflict(&self) {
            self.num_conflicts.set(self.num_conflicts.get() + 1);
        }

        fn record_success(&self) {
            self.num_success.set(self.num_success.get() + 1);
        }

        fn num_conflicts(&self) -> usize {
            self.num_conflicts.get()
        }

        fn num_success(&self) -> usize {
            self.num_success.get()
        }
    }

    #[test]
    fn multi_node_cas_increment() -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..1000 {
            let test_stats = Rc::new(TestStats::new());
            let mut sim = crate::sim::Sim::new();
            let acceptors = TestAcceptors::new(5, 1);
            let mut proposer = acceptors.proposer(ProposerId(1));
            let stats = test_stats.clone();
            sim.add_machine("proposer-1", async move {
                for _ in 0..5 {
                    log::info!("proposer-1: starting inc");
                    let old = inc_and_get(&mut proposer, &stats).await;
                    log::info!("proposer-1: inc success, old value: {}", old);
                }
                Ok(())
            });

            let mut proposer = acceptors.proposer(ProposerId(2));
            let stats = test_stats.clone();
            sim.add_machine("proposer-2", async move {
                for _ in 0..5 {
                    log::info!("proposer-2: starting inc");
                    let old = inc_and_get(&mut proposer, &stats).await;
                    log::info!("proposer-2: inc success, old value: {}", old);
                }
                Ok(())
            });

            let mut proposer = acceptors.proposer(ProposerId(3));
            sim.block_on(async move {
                sim::Handle::current().wait_machines().await;
                let current_val = get(&mut proposer).await;
                // Each proposer will increment the value a minimum of 5 times.
                // The value may be incremented more than 5 times if there are
                // conflicts, so expected is really an upper bound.
                let expected = 5 + 5 + test_stats.num_conflicts();
                assert_eq!(5 + 5, test_stats.num_success());
                assert!(current_val <= expected as u64);
                Ok(())
            })?;
        }
        Ok(())
    }

    #[test]
    fn single_node_cas_increment() -> Result<(), Box<dyn std::error::Error>> {
        init();
        let sim = crate::sim::Sim::new();
        let test_stats = Rc::new(TestStats::new());
        sim.block_on(async move {
            let acceptors = TestAcceptors::new(5, 32);
            let mut proposer = acceptors.proposer(ProposerId(1));
            for i in 0..10 {
                assert_eq!(i, inc_and_get(&mut proposer, &test_stats).await);
            }

            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn acceptor_prepare() -> Result<(), Box<dyn std::error::Error>> {
        let sim = crate::sim::Sim::new();
        sim.block_on(async {
            let acceptor = AcceptorImpl::new(InMemoryRegisters::new(32));
            let p1 = ProposerId(1);
            let slot = Slot(0);

            // First prepare should succeed
            let result = acceptor.prepare(slot, Ballot::new(p1, 0)).await?;
            assert!(matches!(result, PrepareResult::Prepared { .. }));

            // Second prepare at a higher ballot should succeed
            let result = acceptor.prepare(slot, Ballot::new(p1, 1)).await?;
            assert!(
                matches!(result, PrepareResult::Prepared(VersionedValue{ballot,..}) if ballot.is_unknown())
            );

            // Third prepare at a lower ballot should fail
            let result = acceptor.prepare(slot, Ballot::new(p1, 0)).await;
            assert!(matches!(
                result,
                Ok(PrepareResult::Conflict(ballot)) if ballot == Ballot::new(p1, 1)
            ));

            Ok(())
        })?;
        Ok(())
    }
}
