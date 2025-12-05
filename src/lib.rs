use futures::channel::mpsc::{Receiver, Sender, channel};
use futures::future::{MaybeDone, maybe_done};
use futures::{FutureExt, SinkExt, Stream, StreamExt, join};
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

    async fn wait_for_space(&mut self) {
        std::future::poll_fn(|cx| self.sender.poll_ready(cx))
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

    // Drive all the contained futures without popping any outputs. This function never completes,
    // and it's intended to be used in a `select!` or similar with something else that will
    // complete. If any of the contained futures are pending, then they will register for a wakeup.
    // If not (either because all the contained futures are ready, or because this container is
    // empty), then this function's future will yield without ever waking again.
    //
    // This method is cancel-safe.
    fn poll_without_popping(&mut self) -> impl Future {
        std::future::poll_fn::<(), _>(|cx| {
            for fut in &mut self.futures {
                _ = fut.as_mut().poll(cx);
            }
            Poll::Pending
        })
    }

    // Drive all the contained futures until the first one (in order) is ready, and then remove it
    // and return its output. If this container is empty, return `None`.
    //
    // This method is cancel-safe.
    fn pop_front(&mut self) -> impl Future<Output = Option<Fut::Output>> {
        std::future::poll_fn(|cx| {
            if self.futures.is_empty() {
                return Poll::Ready(None);
            }
            // Poll all the futures once. This is a no-op for MaybeDone futures that are already
            // done.
            for fut in &mut self.futures {
                let _ = fut.as_mut().poll(cx);
            }
            // If the first future is done, return its output.
            if let Some(output) = self.futures[0].as_mut().take_output() {
                self.futures.pop_front();
                return Poll::Ready(Some(output));
            }
            // Otherwise at least the first future (and possibly others) has registered a wakeup.
            Poll::Pending
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
                    async move {
                        let mut futures = ConcurrentFutures::new();
                        let mut inputs = self.outputs.fuse();
                        loop {
                            let futures_len = futures.len();
                            let get_input = async {
                                if futures_len < limit {
                                    if let Some(input) = inputs.next().await {
                                        input
                                    } else {
                                        // If the inputs stream is finished, this future never returns.
                                        std::future::pending().await
                                    }
                                } else {
                                    // If we're already at the limit, this future never returns.
                                    std::future::pending().await
                                }
                            };
                            let send_output = async {
                                if futures_len == 0 {
                                    // If we haven't collected any futures yet, this future never
                                    // returns.
                                    std::future::pending().await
                                }
                                // Fusing inside a select! is normally a mistake, but in this case
                                // I claim it's fine.
                                futures::select!(
                                    _ = sender.wait_for_space().fuse() => {}
                                    // `poll_without_popping` makes sure we keep making forward
                                    // progress, but it never returns. Only `wait_for_space` can
                                    // complete this `join!`.
                                    _ = futures.poll_without_popping().fuse() => {}
                                );
                                // At this point we're guaranteed to be able to `send`, so we can
                                // get an output without worrying about dropping it.
                                let output = futures.pop_front().await.expect("there should be futures, we just checked");
                                sender.sender.start_send(output).expect("there should be space, we just checked");
                            };
                            futures::select! {
                                input = pin!(get_input.fuse()) => {
                                    futures.push_back(f(input));
                                },
                                _ = pin!(send_output.fuse()) => {}
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
