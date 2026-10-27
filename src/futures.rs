use std::{
    pin::Pin,
    task::{Context, Poll},
    thread::{self},
    time::{Duration, Instant},
};

use mio::net::TcpStream;
use std::io::{Read, Write};

/*
 ******* Ready Once *******
 */
pub struct ReadyOnce {
    pub(crate) value: usize,
}

impl Future for ReadyOnce {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
        Poll::Ready(self.value)
    }
}

/*
 ******* Poll Once *******
 */
#[derive(Default, Debug)]
pub struct PollOnce {
    is_ready: bool,
}

impl Future for PollOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_ready {
            Poll::Ready(())
        } else {
            self.is_ready = true;
            cx.waker().wake_by_ref(); // Make runtime work on task again
            Poll::Pending
        }
    }
}

/*
 ******* Sleep Timer *******
 */
pub struct SleepFuture {
    duration: Duration,
    deadline: Option<Instant>,
}

impl SleepFuture {
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            deadline: None,
        }
    }
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        } else {
            self.deadline = Some(Instant::now() + self.duration);

            let waker = cx.waker().clone();
            let duration = self.duration;
            thread::spawn(move || {
                thread::sleep(duration);
                waker.wake();
            });

            Poll::Pending
        }
    }
}

/*
 ******* IO Futures *******
 */
pub struct TcpReadFuture<'a> {
    stream: &'a mut TcpStream,
    buf: &'a mut [u8],
}

impl<'a> TcpReadFuture<'a> {
    pub fn new(stream: &'a mut TcpStream, buf: &'a mut [u8]) -> Self {
        Self { stream, buf }
    }
}

impl<'a> std::future::Future for TcpReadFuture<'a> {
    type Output = std::io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        match this.stream.read(this.buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                println!("[IO Future] Blocked on read");
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)), // Return the error
        }
    }
}
