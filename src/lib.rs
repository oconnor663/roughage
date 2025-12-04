use futures::channel::mpsc::{Receiver, Sender, channel};
use futures::{SinkExt, Stream, StreamExt, join};
use std::pin::pin;

fn rendezvous_channel<T>() -> (RendezvousSender<T>, Receiver<T>) {
    let (sender, receiver) = channel(0);
    (RendezvousSender { sender }, receiver)
}

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
            .expect("the recipient should not drop");
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
    pub fn then<U>(mut self, mut f: impl AsyncFnMut(T) -> U) -> AsyncPipeline<impl Future, U> {
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

    pub async fn for_each(mut self, mut f: impl AsyncFnMut(T)) {
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
}
