use futures::channel::mpsc::{Receiver, Sender, channel};
use futures::future::{MaybeDone, maybe_done};
use futures::{SinkExt, Stream, StreamExt, join};
use join_me_maybe::join_me_maybe;
use std::collections::VecDeque;
use std::pin::{Pin, pin};
use std::task::Poll;

fn rendezvous_channel<T>() -> (RendezvousSender<T>, Receiver<T>) {
    let (sender, receiver) = channel(0);
    (RendezvousSender { sender }, receiver)
}

#[derive(Clone)]
struct RendezvousSender<T> {
    sender: Sender<T>,
}

impl<T> RendezvousSender<T> {
    // Send an item into the `channel(0)` (which can buffer one item) and then wait for the
    // recipient to clear it. The wait is what makes this a "rendezvous" channel.
    async fn send_and_wait(&mut self, item: T) {
        // Internally `futures::channel::mpsc::Sender::send` does a `flush` after sending, so we
        // don't actually need to do anything extra here to wait for the buffer to clear. However,
        // I find that surprising enough that I think it's worth it to wrap it in this method. See
        // `test_rendezvous`.
        self.sender
            .send(item)
            .await
            .expect("receiver should not drop");
    }
}

struct ConcurrentFutures<Fut: Future> {
    futures: VecDeque<Pin<Box<MaybeDone<Fut>>>>,
}

impl<Fut: Future> ConcurrentFutures<Fut> {
    fn new() -> Self {
        Self {
            futures: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.futures.len()
    }

    fn push_back(&mut self, fut: Fut) {
        self.futures.push_back(Box::pin(maybe_done(fut)));
    }

    // Drive all the contained futures until the first one (in order) is ready, then remove it and
    // send its output to the provided channel. Of if this container is empty, return immediately.
    //
    // This method is **cancel-safe**. In particular, it waits until there's space in the output
    // channel before taking the first future's output.
    fn await_and_send_first_output(
        &mut self,
        sender: &mut Sender<Fut::Output>,
    ) -> impl Future<Output = ()> {
        std::future::poll_fn(|cx| {
            let mut futures_iter = self.futures.iter_mut();
            // If the collection is empty, short-circuit.
            let Some(first) = futures_iter.next() else {
                return Poll::Pending;
            };
            // If the first future isn't ready, we won't poll the channel, because there's no point
            // in letting it wake us up yet. In this case poll all the other `MaybeDone` futures
            // and short-circuit. (This is critical for concurrency. Also note that polling a
            // `MaybeDone` that's already done is a no-op.)
            if first.as_mut().poll(cx).is_pending() {
                for fut in futures_iter {
                    _ = fut.as_mut().poll(cx);
                }
                return Poll::Pending;
            }
            // If the channel isn't ready, then again we'll poll all the other futures once and
            // short-circuit. In this case the channel will eventually wake us up.
            if !sender.poll_ready(cx).is_ready() {
                for fut in futures_iter {
                    _ = fut.as_mut().poll(cx);
                }
                return Poll::Pending;
            }
            // If the first future is ready *and* the channel is ready, then we can do a
            // cancel-safe send. In this case we don't need to poll the other futures, because
            // we're going to return Ready to the caller, so the caller won't assume that any
            // wakeups are scheduled. (In practice, the caller will loop around and call this whole
            // function again, possibly after pushing a new future in.)
            let output = first
                .as_mut()
                .take_output()
                .expect("already checked output ready");
            sender
                .start_send(output)
                .expect("already checked channel ready, and it should never close");
            self.futures.pop_front();
            Poll::Ready(())
        })
    }
}

pub fn pipeline<S: Stream>(stream: S) -> AsyncPipeline<impl Future, S::Item> {
    let (mut sender, receiver) = rendezvous_channel();
    AsyncPipeline {
        future: async move {
            let mut stream = pin!(stream);
            while let Some(item) = stream.next().await {
                sender.send_and_wait(item).await;
            }
        },
        outputs: receiver,
    }
}

pub struct AsyncPipeline<Fut: Future, T> {
    future: Fut,
    outputs: Receiver<T>,
}

impl<Fut: Future, T> AsyncPipeline<Fut, T> {
    pub fn then<F, U>(mut self, mut f: F) -> AsyncPipeline<impl Future, U>
    where
        F: AsyncFnMut(T) -> U,
    {
        let (mut sender, receiver) = rendezvous_channel();
        AsyncPipeline {
            future: async {
                join! {
                    self.future,
                    async move {
                        while let Some(input) = self.outputs.next().await {
                            let output = f(input).await;
                            sender.send_and_wait(output).await;
                        }
                    }
                };
            },
            outputs: receiver,
        }
    }

    pub fn then_concurrent<F, U>(self, f: F, limit: usize) -> AsyncPipeline<impl Future, U>
    where
        F: AsyncFn(T) -> U,
    {
        assert!(limit > 0, "`limit` must be at least 1");
        let (mut sender, receiver) = rendezvous_channel();
        AsyncPipeline {
            future: async move {
                join! {
                    self.future,
                    async {
                        let mut futures = ConcurrentFutures::new();
                        let mut inputs = self.outputs.fuse();
                        loop {
                            let futures_len = futures.len();
                            let (maybe_input, _) = join_me_maybe!(
                                async {
                                    if futures_len == limit {
                                        // The collection is full. Don't take any input until the
                                        // next output is sent. Note that `return` ends this async
                                        // block, not the whole join or the whole function.
                                        return None;
                                    }
                                    // If we get an input, cancel the send side so that we can add
                                    // a new future to the collection immediately. Otherwise, let
                                    // the send side run.
                                    if let Some(input) = inputs.next().await {
                                        send_output.cancel();
                                        Some(input)
                                    } else {
                                        None
                                    }
                                },
                                send_output: futures.await_and_send_first_output(&mut sender.sender),
                            );
                            if let Some(input) = maybe_input {
                                futures.push_back(f(input));
                            }
                            if inputs.is_done() && futures.len() == 0 {
                                return;
                            }
                        }
                    }
                };
            },
            outputs: receiver,
        }
    }

    pub fn buffered(mut self, buf_size: usize) -> AsyncPipeline<impl Future, T> {
        assert!(buf_size > 0, "`buf_size` must be at least 1");
        let (mut sender, receiver) = channel(buf_size);
        AsyncPipeline {
            future: async {
                join! {
                    self.future,
                    async move {
                        while let Some(input) = self.outputs.next().await {
                            sender.send(input).await.expect("receiver should not drop");
                        }
                    }
                };
            },
            outputs: receiver,
        }
    }

    pub async fn for_each<F>(mut self, mut f: F)
    where
        F: AsyncFnMut(T),
    {
        join! {
            self.future,
            async move {
                while let Some(input) = self.outputs.next().await {
                    f(input).await;
                }
            }
        };
    }

    pub async fn collect<C: Default + Extend<T>>(mut self) -> C {
        join! {
            self.future,
            async move {
                let mut collection = C::default();
                while let Some(input) = self.outputs.next().await {
                    collection.extend(std::iter::once(input));
                }
                collection
            }
        }
        .1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_for_each() {
        let mut v = Vec::new();
        pipeline(futures::stream::iter(0..5))
            .then(async |x| {
                sleep(Duration::from_millis(1)).await;
                x + 1
            })
            .then(async |x| {
                sleep(Duration::from_millis(1)).await;
                10 * x
            })
            .for_each(async |x| {
                v.push(x);
            })
            .await;
        assert_eq!(v, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test]
    async fn test_collect() {
        let v: Vec<_> = pipeline(futures::stream::iter(0..5))
            .then(async |x| {
                sleep(Duration::from_millis(1)).await;
                x + 1
            })
            .then(async |x| {
                sleep(Duration::from_millis(1)).await;
                10 * x
            })
            .collect()
            .await;
        assert_eq!(v, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test]
    async fn test_rendezvous() {
        use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
        static ELEMENTS_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        let mut i = 0;
        pipeline(futures::stream::iter(std::iter::from_fn(|| {
            if i < 10 {
                let in_flight = ELEMENTS_IN_FLIGHT.fetch_add(1, Relaxed);
                assert_eq!(in_flight, 0, "too many elements in flight at i = {i}");
                i += 1;
                Some(i)
            } else {
                None
            }
        })))
        .for_each(async |i| {
            let in_flight = ELEMENTS_IN_FLIGHT.fetch_sub(1, Relaxed);
            assert_eq!(in_flight, 1, "too many elements in flight at i = {i}");
            sleep(Duration::from_millis(1)).await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_buffered() {
        use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
        static ELEMENTS_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        let mut i = 0u32;
        pipeline(futures::stream::iter(std::iter::from_fn(|| {
            if i < 10 {
                let in_flight = ELEMENTS_IN_FLIGHT.fetch_add(1, Relaxed);
                // This test might be a little too sensitive, but when the behavior changes I want
                // to see it.
                if i <= 4 {
                    assert_eq!(in_flight, i.saturating_sub(1), "i = {i}");
                } else {
                    assert_eq!(in_flight, 4, "i = {i}");
                }
                i += 1;
                Some(i)
            } else {
                None
            }
        })))
        .buffered(3)
        .for_each(async |_| {
            ELEMENTS_IN_FLIGHT.fetch_sub(1, Relaxed);
            sleep(Duration::from_millis(1)).await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_concurrent() {
        use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
        static FUTURES_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        static MAX_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        let v: Vec<i32> = pipeline(futures::stream::iter(0..10))
            .then_concurrent(
                async |i| {
                    let in_flight = FUTURES_IN_FLIGHT.fetch_add(1, Relaxed);
                    MAX_IN_FLIGHT.fetch_max(in_flight + 1, Relaxed);
                    sleep(Duration::from_millis(1)).await;
                    FUTURES_IN_FLIGHT.fetch_sub(1, Relaxed);
                    2 * i
                },
                3,
            )
            .collect()
            .await;
        assert_eq!(v, vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18]);
    }
}
