use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::rc::Rc;

use bytes::Bytes;
use tokio::sync::Notify;

use crate::net::{Error, Network};
use crate::NodeId;

#[derive(Debug)]
pub(crate) struct SimNetwork {
    state: Rc<State>,
}

impl Clone for SimNetwork {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl SimNetwork {
    pub(crate) fn new() -> Self {
        Self {
            state: Rc::new(State::new()),
        }
    }

    pub(crate) fn handle(&self, id: NodeId) -> SimNetworkHandle {
        self.state.add_node(id);
        SimNetworkHandle {
            id,
            state: self.state.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SimNetworkHandle {
    id: NodeId,
    state: Rc<State>,
}

impl Clone for SimNetworkHandle {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            state: self.state.clone(),
        }
    }
}

#[derive(Debug)]
struct State {
    senders: RefCell<HashMap<NodeId, Sender<(NodeId, Bytes)>>>,
    receivers: RefCell<HashMap<NodeId, Receiver<(NodeId, Bytes)>>>,
}

impl State {
    fn new() -> Self {
        Self {
            senders: RefCell::new(HashMap::new()),
            receivers: RefCell::new(HashMap::new()),
        }
    }

    fn add_node(&self, id: NodeId) {
        if self.senders.borrow().contains_key(&id) {
            return;
        }
        let (sender, receiver) = new_queue();
        self.senders.borrow_mut().insert(id, sender);
        self.receivers.borrow_mut().insert(id, receiver);
    }
}

impl Network for SimNetworkHandle {
    async fn send(&self, id: NodeId, req: Bytes) -> Result<(), Error> {
        let senders = self.state.senders.borrow();
        let Some(sender) = senders.get(&id) else {
            // TODO: Maybe this should just return Ok sometimes.
            return Err(crate::net::Error::Io(io::Error::from_raw_os_error(113)));
        };
        sender.send((self.id, req));
        Ok(())
    }

    async fn recv(&self) -> Result<(NodeId, Bytes), Error> {
        let receiver = {
            let receivers = self.state.receivers.borrow();
            receivers.get(&self.id).unwrap().clone()
        };
        Ok(receiver.recv().await)
    }
}

fn new_queue<T>() -> (Sender<T>, Receiver<T>) {
    let queue = Rc::new(MsgQueue::new());
    let sender = Sender {
        inner: queue.clone(),
    };
    let receiver = Receiver { inner: queue };
    (sender, receiver)
}

#[derive(Debug)]
struct Sender<T> {
    inner: Rc<MsgQueue<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Sender<T> {
    fn send(&self, msg: T) {
        self.inner.push(msg);
    }
}

#[derive(Debug)]
struct Receiver<T> {
    inner: Rc<MsgQueue<T>>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Receiver<T> {
    async fn recv(&self) -> T {
        self.inner.pop().await
    }
}

#[derive(Debug)]
struct MsgQueue<T> {
    notify: Notify,
    queue: RefCell<VecDeque<T>>,
}

impl<T> MsgQueue<T> {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            queue: RefCell::new(VecDeque::new()),
        }
    }

    fn push(&self, msg: T) {
        self.queue.borrow_mut().push_back(msg);
        self.notify.notify_one();
    }

    async fn pop(&self) -> T {
        loop {
            if let Some(msg) = self.try_pop() {
                return msg;
            } else {
                self.notify.notified().await;
            }
        }
    }

    fn try_pop(&self) -> Option<T> {
        self.queue.borrow_mut().pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn net_send_rx() -> Result<(), Box<dyn std::error::Error>> {
        let net = SimNetwork::new();
        let sim = crate::sim::Sim::new();

        sim.block_on(async move {
            let h1 = net.handle(NodeId(1));
            let h2 = net.handle(NodeId(2));

            h1.send(NodeId(2), "hello".into()).await?;
            let (id, msg) = h2.recv().await?;
            assert_eq!(id, NodeId(1));
            assert_eq!(msg, "hello");
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn host_unreachable() -> Result<(), Box<dyn std::error::Error>> {
        let net = SimNetwork::new();
        let sim = crate::sim::Sim::new();

        sim.block_on(async move {
            let h1 = net.handle(NodeId(1));

            let err = h1.send(NodeId(3), "hello".into()).await.unwrap_err();
            assert_eq!(err.to_string(), "No route to host (os error 113)");
            Ok(())
        })?;
        Ok(())
    }
}
