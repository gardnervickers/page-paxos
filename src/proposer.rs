use std::future::Future;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::acceptor::{AcceptResult, Acceptor, PrepareResult, VersionedValue};
use crate::{Ballot, ProposerId, Slot};

#[derive(Debug)]
pub(crate) struct Proposer<A> {
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

    pub(crate) fn id(&self) -> ProposerId {
        self.id
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
        self.cache_ballot(slot, new_ballot);
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
        log::info!(
            "{:?} last_value={:?} => new_value={:?}",
            self.id,
            last_val,
            new_value
        );

        // 2. Accept Phase:
        //    Send the new value to the acceptors. The goal here is to confirm the
        //    work that we performed above. If we are successful, we will return an
        //    error.
        let new_value = VersionedValue::new(new_ballot, new_value);
        self.accept(slot, new_value).await?;
        Ok(last_val)
    }

    /// Execute the prepare phase for all acceptors.
    ///
    /// This will prepare a new ballot for the given slot by sending a prepare message
    /// to all acceptors.
    ///
    /// On success, returns a tuple of the highest known value for the slot and the
    /// new ballot number. The new ballot number will be higher than any previously seen
    /// ballot numbers and will be owned by this proposer.
    async fn prepare(&self, slot: Slot) -> Result<(VersionedValue, Ballot), Error> {
        let mut last_known_ballot = self.new_ballot_for_slot(slot);
        loop {
            // TODO: Add backoff/retry and some limit on the number of retries.
            let propose_ballot = last_known_ballot.increment(self.id);

            // Send prepare messages to all acceptors.
            let futs = FuturesUnordered::new();
            for acceptor in &self.acceptors {
                let fut = acceptor.prepare(slot, propose_ballot);
                futs.push(fut);
            }

            // Wait for a majority of responses.
            let promises = wait_for(futs, self.majority(), |v| v.value().is_some()).await;
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

            // Majority was not reached. Set the last known ballot to the highest
            // ballot number returned by the acceptors and retry.
            let max_ballot = promises
                .into_iter()
                .filter_map(|res| res.ok())
                .filter_map(|res| res.conflict())
                .max()
                .unwrap_or(propose_ballot);
            last_known_ballot = max_ballot;
            continue;
        }
    }

    /// Execute the accept phase for all acceptors.
    ///
    /// This will send an accept message to all acceptors. If a majority of acceptors
    /// respond positively, Ok will be returned. If we fail
    /// to reach a majority, an error will be returne instead.
    async fn accept(&self, slot: Slot, value: VersionedValue) -> Result<(), Error> {
        let futs = FuturesUnordered::new();

        // Send accept messages to all acceptors.
        for acceptor in &self.acceptors {
            let fut = acceptor.accept(slot, value.clone());
            futs.push(fut);
        }
        // Wait for a majority of responses.
        let promises = wait_for(futs, self.majority(), |v| v.is_accepted()).await;
        let nr_success = promises
            .iter()
            .filter(|res| matches!(res, Ok(AcceptResult::Accepted { .. })))
            .count();
        if nr_success >= self.majority() {
            Ok(())
        } else {
            Err(Error::Conflict)
        }
    }

    fn new_ballot_for_slot(&self, _slot: Slot) -> Ballot {
        // TODO: Use this for a cache lookup at some point.
        Ballot::new(self.id, 0)
    }

    fn cache_ballot(&mut self, _slot: Slot, _new_ballot: Ballot) {
        // TODO: Use this to store ballots in the cache.
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

/// Wait for a majority of successful responses.
///
/// This will wait for a majority of successful responses to come back.
/// The full list of all responses will be returned.
async fn wait_for<U, E>(
    mut futures: FuturesUnordered<impl Future<Output = Result<U, E>>>,
    majority: usize,
    is_success: impl Fn(&U) -> bool,
) -> Vec<Result<U, E>> {
    let mut results = Vec::new();
    let mut num_success = 0;
    while let Some(result) = futures.next().await {
        if let Ok(res) = &result {
            if is_success(res) {
                num_success += 1;
            }
        }
        results.push(result);
        if num_success >= majority {
            break;
        }
    }
    results
}
