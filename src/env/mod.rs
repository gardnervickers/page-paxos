//! Environment trait which is used to provide access to
//! system resources.
use std::time::Instant;

use futures::Future;
use hyper::rt;

/// [`Env`] is an abstraction over the system resources
///
/// This allows for swapping out the underlying executor.
pub(crate) trait Env: 'static {
    type JoinHandle<U>: Abortable + Future<Output = U> + 'static
    where
        U: 'static;

    type Network: crate::net::Network + 'static;

    fn spawn_local<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static;

    fn time(&self) -> impl rt::Timer;

    fn now(&self) -> Instant;

    fn rand(&self) -> impl rand::Rng;

    fn net(&self) -> Self::Network;

    fn node_id(&self) -> crate::NodeId;
}

pub(crate) trait Abortable {
    fn abort(&self);
}
