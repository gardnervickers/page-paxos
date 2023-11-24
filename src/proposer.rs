use std::future::Future;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::acceptor::{AcceptResult, Acceptor, PrepareResult, VersionedValue};
use crate::{Ballot, ProposerId, Slot};

pub(crate) struct Proposer<A> {
    // TODO: We should keep a per-proposer cache of
    //       ballots and values. This would allow us to
    //       do something like the preferred leader optimization
    //       where acceptors automatically run the prepare phase
    //       on behalf of the last proposer.
    id: ProposerId,
    acceptors: Vec<A>,
}

impl<A> Proposer<A>
where
    A: Acceptor,
{
    pub(crate) fn new(id: ProposerId, acceptors: Vec<A>) -> Self {
        Self { id, acceptors }
    }
    /// Perform a consensus operation on the set of acceptors.
    ///
    /// The CAS function is called with the current value of the slot and
    /// a transform function. The transform function will be applied to the
    /// value and the result will be sent to the acceptors. This method
    /// will return the last committed value of the slot.
    pub(crate) async fn cas<F>(&mut self, slot: Slot, mut xform: F) -> Result<VersionedValue, Error>
    where
        F: FnMut(Option<&[u8]>) -> Option<Vec<u8>>,
    {
        // 1. Prepare Phase:
        //    The goal here is to land on a ballot number that is higher than any
        //    previously seen ballot number for this slot. Once found, it will be returned
        //    along with the highest known value for the slot.
        let (last_val, new_ballot) = self.prepare(slot).await?;
        assert_eq!(new_ballot.0, self.id);
        log::info!(
            "{:?} prepare success slot={slot:?} last_ballot={:?}, new_ballot={new_ballot:?}",
            self.id,
            last_val.ballot(),
        );

        // Transform the value using the CAS function.
        //
        // TODO: We could actually store a queue of CAS functions and apply them
        //       in any order. This would allow us to "batch" CAS operations in
        //       the same way that we can batch log appends in a log oriented consensus
        //       protocol.
        let new_value = (xform)(last_val.value());

        // 2. Accept Phase:
        //    Send the new value to the acceptors. The goal here is to confirm the
        //    work that we performed above. If we are successful, we will return an
        //    error.
        let new_value = VersionedValue::new(new_ballot, new_value);
        let _new_value = self.accept(slot, new_value).await?;
        Ok(last_val)
    }

    async fn accept(&mut self, slot: Slot, value: VersionedValue) -> Result<VersionedValue, Error> {
        let futs = FuturesUnordered::new();

        // Send accept messages to all acceptors.
        for acceptor in &self.acceptors {
            let fut = acceptor.accept(slot, value.clone());
            futs.push(fut);
        }
        // Wait for a majority of responses.
        let promises = wait_for(futs, self.majority()).await;
        let nr_success = promises
            .iter()
            .filter(|res| matches!(res, Ok(AcceptResult::Accepted { .. })))
            .count();
        if nr_success >= self.majority() {
            Ok(value)
        } else {
            Err(Error::Conflict)
        }
    }

    /// Run through the prepare phase.
    ///
    /// Returns the highest known value for the slot.
    async fn prepare(&mut self, slot: Slot) -> Result<(VersionedValue, Ballot), Error> {
        let mut last_known_ballot = self.new_ballot_for_slot(slot);
        loop {
            let propose_ballot = last_known_ballot.increment(self.id);

            // Send prepare messages to all acceptors.
            let futs = FuturesUnordered::new();
            for acceptor in &self.acceptors {
                let fut = acceptor.prepare(slot, propose_ballot);
                futs.push(fut);
            }

            // Wait for a majority of responses.
            let promises = wait_for(futs, self.majority()).await;
            let nr_success = promises
                .iter()
                .filter(|res| matches!(res, Ok(PrepareResult::Prepared { .. })))
                .count();

            if nr_success >= self.majority() {
                // Majority was reached, return the highest known value.
                let value = promises
                    .into_iter()
                    .filter_map(|res| res.ok())
                    .filter_map(|res| res.into_value())
                    .max()
                    .unwrap();
                return Ok((value, propose_ballot));
            }
            // Majority was not reached. Blindly increment the ballot and try again.
            //
            // This is inefficient but correct, really we should look at the responses
            // and pick a ballot number from them.
            last_known_ballot = propose_ballot;
            continue;
        }
    }

    fn new_ballot_for_slot(&self, _slot: Slot) -> Ballot {
        // TODO: Use this for a cache lookup at some point.
        Ballot::new(self.id, 0)
    }

    fn majority(&self) -> usize {
        self.acceptors.len() / 2 + 1
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("a conflicting value was already committed")]
    Conflict,
}

async fn wait_for<U>(
    mut futures: FuturesUnordered<impl Future<Output = U>>,
    majority: usize,
) -> Vec<U> {
    let mut results = Vec::new();
    while let Some(result) = futures.next().await {
        results.push(result);
        if results.len() == majority {
            return results;
        }
    }
    results
}
