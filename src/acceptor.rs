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
        let mut register = self.registers.read(slot).await;
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

        let result = PrepareResult::Prepared(value);
        self.registers.write(slot, register).await;
        Ok(result)
    }

    async fn accept(&self, slot: Slot, value: VersionedValue) -> Result<AcceptResult, Error> {
        // 1. Read the register, return a conflict if it already saw a greater ballot number.
        let mut register = self.registers.read(slot).await;
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
        self.registers.write(slot, register).await;
        Ok(AcceptResult::Accepted)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PrepareResult {
    Prepared(VersionedValue),
    Conflict(Ballot),
}

impl PrepareResult {
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

    async fn read(&self, slot: Slot) -> Register;

    async fn write(&self, slot: Slot, register: Register);
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use tokio::sync::Mutex;

    use crate::proposer::Proposer;
    use crate::ProposerId;

    use super::*;

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

        async fn read(&self, slot: Slot) -> Register {
            let r = self.registers.lock().await[slot.0 as usize].clone();
            r
        }

        async fn write(&self, slot: Slot, register: Register) {
            self.registers.lock().await[slot.0 as usize] = register;
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
                .map(|v| Rc::new(v))
                .collect();
            Self { acceptors }
        }

        fn proposer(&self, id: ProposerId) -> Proposer<Rc<AcceptorImpl<InMemoryRegisters>>> {
            Proposer::new(id, self.acceptors.clone())
        }
    }

    // async fn inc_and_get(pid: ProposerId, acceptors: &Acceptors) -> u64 {
    //     let ballot = Ballot::new(pid, 0);
    //     let res = cas(&acceptors.acceptors, SlotId(0), ballot, |old| match old {
    //         Some(val) => {
    //             let existing = u64::from_be_bytes(val.try_into().unwrap());
    //             let new = existing + 1;
    //             Some(new.to_be_bytes().to_vec())
    //         }
    //         None => Some(1u64.to_be_bytes().to_vec()),
    //     })
    //     .await;
    //     res.map(|val| u64::from_be_bytes(val.try_into().unwrap()))
    //         .unwrap_or(0)
    // }

    // #[test]
    // fn multi_node_cas_increment() -> Result<(), Box<dyn std::error::Error>> {
    //     init();
    //     let mut sim = crate::sim::Sim::new();
    //     let acceptors = Acceptors::new(3, 32);

    //     let acc = acceptors.clone();
    //     sim.add_machine("proposer-1", async move {
    //         let p1 = ProposerId(1);
    //         for i in 0..10 {
    //             assert_eq!(i, inc_and_get(p1, &acc).await);
    //         }
    //         Ok(())
    //     });

    //     let acc = acceptors.clone();
    //     sim.add_machine("proposer-2", async move {
    //         let p1 = ProposerId(2);
    //         for i in 0..10 {
    //             assert_eq!(i, inc_and_get(p1, &acc).await);
    //         }
    //         Ok(())
    //     });

    //     sim.block_on(async {
    //         time::sleep(Duration::from_secs(1000)).await;
    //         Ok(())
    //     })?;
    //     Ok(())
    // }

    async fn inc_and_get<A>(proposer: &mut Proposer<A>) -> u64
    where
        A: Acceptor,
    {
        proposer
            .cas(Slot(0), |old| match old {
                Some(old_val) => {
                    let existing = u64::from_be_bytes(old_val.try_into().unwrap());
                    let new = existing + 1;
                    Some(new.to_be_bytes().to_vec())
                }
                None => Some(1u64.to_be_bytes().to_vec()),
            })
            .await
            .unwrap()
            .value()
            .map(|v| u64::from_be_bytes(v.try_into().unwrap()))
            .unwrap_or(0)
    }

    #[test]
    fn single_node_cas_increment() -> Result<(), Box<dyn std::error::Error>> {
        init();
        let sim = crate::sim::Sim::new();
        sim.block_on(async {
            let acceptors = TestAcceptors::new(5, 32);
            let mut proposer = acceptors.proposer(ProposerId(1));
            for i in 0..10 {
                assert_eq!(i, inc_and_get(&mut proposer).await);
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
