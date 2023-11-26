use std::panic;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Instant;

use futures::Future;
use hyper::rt;

use crate::env::Abortable;

pub(crate) struct TokioJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> TokioJoinHandle<T> {
    pub(crate) fn new(inner: tokio::task::JoinHandle<T>) -> Self {
        Self(inner)
    }
}

impl<T> std::future::Future for TokioJoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(Pin::new(&mut self.0).poll(cx)) {
            Ok(ok) => Poll::Ready(ok),
            Err(err) => {
                if err.is_panic() {
                    panic::resume_unwind(err.into_panic());
                } else {
                    panic!("unexpected join error: {:?}", err);
                }
            }
        }
    }
}

impl<U> Abortable for TokioJoinHandle<U> {
    fn abort(&self) {
        self.0.abort()
    }
}

impl Abortable for tokio::task::JoinHandle<()> {
    fn abort(&self) {
        tokio::task::JoinHandle::abort(self)
    }
}

pin_project_lite::pin_project! {
    struct TokioSleep {
        #[pin]
        inner: tokio::time::Sleep,
    }
}

impl Future for TokioSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

impl TokioSleep {
    fn reset(self: Pin<&mut Self>, deadline: Instant) {
        self.project().inner.as_mut().reset(deadline.into());
    }
}

impl rt::Sleep for TokioSleep {}

pub(crate) struct TokioTimer;
impl rt::Timer for TokioTimer {
    fn sleep(&self, duration: std::time::Duration) -> Pin<Box<dyn rt::Sleep>> {
        Box::pin(TokioSleep {
            inner: tokio::time::sleep(duration),
        })
    }

    fn sleep_until(&self, deadline: std::time::Instant) -> Pin<Box<dyn rt::Sleep>> {
        Box::pin(TokioSleep {
            inner: tokio::time::sleep_until(deadline.into()),
        })
    }

    fn reset(&self, sleep: &mut Pin<Box<dyn rt::Sleep>>, new_deadline: std::time::Instant) {
        if let Some(sleep) = sleep.as_mut().downcast_mut_pin::<TokioSleep>() {
            sleep.reset(new_deadline)
        }
    }
}
