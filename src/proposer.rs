use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use hyper::rt::Timer;

use crate::acceptor::{AcceptResult, Acceptor, PrepareResult};
use crate::sim::buggify;
use crate::{Ballot, NodeId, Slot, Versioned};

static LOG: &str = "proposer";

#[derive(Debug)]
pub(crate) struct Proposer<A, T> {
    id: NodeId,
    // TODO: Really we could split this into a prepare and accept set, each
    // with their own quorum sizes.
    //
    // We would just make sure that the prepare set overlaps with the accept
    // set. For example we could have a 5 node cluster with 4 prepare nodes
    // and 2 accept nodes.
    acceptors: Vec<A>,
    ballot_cache: HashMap<Slot, Ballot>,
    timer: T,
}

impl<A, T> Proposer<A, T>
where
    A: Acceptor,
    T: Timer,
{
    pub(crate) fn new(id: NodeId, acceptors: Vec<A>, timer: T) -> Self {
        Self {
            id,
            acceptors,
            ballot_cache: Default::default(),
            timer,
        }
    }

    pub(crate) fn id(&self) -> NodeId {
        self.id
    }
    /// Perform a consensus operation on the set of acceptors.
    ///
    /// The CAS function is called with the current value of the slot and
    /// a transform function. The transform function will be applied to the
    /// value and the result will be sent to the acceptors. This method
    /// will return the last committed value of the slot.
    pub(crate) async fn cas<F>(
        &mut self,
        slot: Slot,
        mut xform: F,
    ) -> Result<Versioned<Vec<u8>>, Error>
    where
        F: FnMut(Option<&[u8]>) -> Option<Vec<u8>>,
    {
        // Randomize the order of the acceptors to increase the likelihood of
        // different acceptors being used for the prepare and accept phases.
        buggify::shuffle(&mut self.acceptors);

        // 1. Prepare Phase:
        //    The goal here is to land on a ballot number that is higher than any
        //    previously seen ballot number for this slot. Once found, it will be returned
        //    along with the highest known value for the slot.
        let (last_val, new_ballot) = self.prepare(slot).await?;
        assert_eq!(new_ballot.0, self.id);

        // Transform the value using the CAS function.
        let new_value = (xform)(last_val.value().map(|v| v.as_slice()));

        // Shuffle the acceptors between stages to increase the likelihood of
        // accepts landing on a different set of acceptors than were used for the
        // prepare phase.
        buggify::shuffle(&mut self.acceptors);

        tracing::debug!(target: LOG,
            new = ?new_ballot,
            old = ?last_val.ballot(),
            "cas.accept.start"
        );

        // 2. Accept Phase:
        //    Send the new value to the acceptors. The goal here is to confirm the
        //    work that we performed above. If we are successful, we will return an
        //    error.
        let new_value = Versioned::new(new_ballot, new_value);
        self.accept(slot, new_value).await?;

        tracing::debug!(target: LOG,
            new = ?new_ballot,
            old = ?last_val.ballot(),
            "cas.accept.success"
        );
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
    #[tracing::instrument(skip(self))]
    async fn prepare(&mut self, slot: Slot) -> Result<(Versioned<Vec<u8>>, Ballot), Error> {
        loop {
            // TODO: Add backoff/retry and some limit on the number of retries.
            let propose_ballot = self.new_ballot_for_slot(slot);

            // Send prepare messages to all acceptors.
            tracing::debug!(target: LOG, ?propose_ballot, "prepare.request");
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
            tracing::debug!(target: LOG, ?propose_ballot, ?nr_success, nr_sends = promises.len(), "prepare.request.complete");

            if nr_success >= self.majority() {
                // Majority was reached, return the highest known value.
                let value = promises
                    .into_iter()
                    .filter_map(|res| res.ok())
                    .filter_map(|res| res.into_value())
                    .max()
                    .unwrap();
                tracing::debug!(target: LOG, ?propose_ballot, ?value, "prepare.success");
                self.cache_ballot(slot, propose_ballot);

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
            tracing::debug!(target: LOG, ?propose_ballot, ?max_ballot, "prepare.conflict");
            self.cache_ballot(slot, max_ballot);

            // Sleep for a bit and retry.
            self.timer.sleep(Duration::from_millis(10)).await;
            continue;
        }
    }

    /// Execute the accept phase for all acceptors.
    ///
    /// This will send an accept message to all acceptors. If a majority of acceptors
    /// respond positively, Ok will be returned. If we fail
    /// to reach a majority, an error will be returne instead.
    #[tracing::instrument(skip(self))]
    async fn accept(&self, slot: Slot, value: Versioned<Vec<u8>>) -> Result<(), Error> {
        let futs = FuturesUnordered::new();

        tracing::debug!(target: LOG, ?value, "accept.request");
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

        tracing::debug!(target: LOG, ?value, ?nr_success, nr_sends = promises.len(), "accept.request.complete");
        if nr_success >= self.majority() {
            Ok(())
        } else {
            Err(Error::Conflict)
        }
    }

    fn new_ballot_for_slot(&self, slot: Slot) -> Ballot {
        if let Some(existing) = self.ballot_cache.get(&slot) {
            existing.increment(self.id)
        } else {
            Ballot::new(self.id, 0)
        }
    }

    fn cache_ballot(&mut self, slot: Slot, new_ballot: Ballot) {
        if let Some(existing) = self.ballot_cache.get(&slot) {
            if existing >= &new_ballot {
                return;
            }
        }
        self.ballot_cache.insert(slot, new_ballot);
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
