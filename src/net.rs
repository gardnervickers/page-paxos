use bytes::Bytes;

use crate::NodeId;

pub(crate) trait Network {
    /// Send a message to a node.
    ///
    /// This has the same gurantees as a UDP socket, but it may be
    /// implemented over a more reliable protocol.
    async fn send(&self, id: NodeId, req: Bytes) -> Result<(), Error>;

    /// Receive a message from a node.
    ///
    /// This has the same gurantees as a UDP socket, but it may be
    /// implemented over a more reliable protocol.
    async fn recv(&self) -> Result<(NodeId, Bytes), Error>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Io(std::io::Error),
    #[error(transparent)]
    Other(Box<dyn std::error::Error>),
}
