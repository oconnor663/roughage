use futures::channel::mpsc::{Receiver, Sender, channel};
use futures::future::join;
use futures::stream::{FusedStream, FuturesOrdered, FuturesUnordered};
use futures::{SinkExt, Stream, StreamExt};
use std::collections::VecDeque;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Poll::Pending, Poll::Ready};

enum OrderedOrUnorderedFutures<Fut: Future> {
    Ordered(FuturesOrdered<Fut>),
    Unordered(FuturesUnordered<Fut>),
}

impl<Fut: Future> OrderedOrUnorderedFutures<Fut> {
    fn len(&self) -> usize {
        match self {
            Self::Ordered(futures) => futures.len(),
            Self::Unordered(futures) => futures.len(),
        }
    }

    fn push(&mut self, fut: Fut) {
        match self {
            Self::Ordered(futures) => {
                futures.push_back(fut);
            }
            Self::Unordered(futures) => {
                futures.push(fut);
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Fut::Output>> {
        match self {
            Self::Ordered(futures) => Pin::new(futures).poll_next(cx),
            Self::Unordered(futures) => Pin::new(futures).poll_next(cx),
        }
    }
}

/// Note that `Fut` here is assumed to be a filter-map. If it returns `Some(U)`, then we unwrap the
/// output before buffering it. If it returns `None`, then we don't buffer it at all.
struct InnerExecutor<Fut: Future<Output = Option<U>>, U> {
    futures: OrderedOrUnorderedFutures<Fut>,
    outputs: VecDeque<U>,
}

impl<Fut: Future<Output = Option<U>>, U> InnerExecutor<Fut, U> {
    fn len(&self) -> usize {
        self.futures.len() + self.outputs.len()
    }

    fn capacity(&self) -> usize {
        self.outputs.capacity()
    }

    fn is_full(&self) -> bool {
        assert!(self.len() <= self.capacity());
        self.len() == self.capacity()
    }

    fn push(&mut self, future: Fut) {
        assert!(!self.is_full());
        self.futures.push(future);
    }

    /// Poll each of the buffered futures (making progress and registering wakeups) without
    /// consuming any of their outputs, for example while the caller is waiting for space in the
    /// outputs channel.
    ///
    /// This method is aware that its inner futures are filter-maps. Their outputs are `Option<U>`,
    /// and when one of them yields `None`, that ouput doesn't get buffered and doesn't count
    /// against the capacity.
    ///
    /// The name of this method comes from: https://without.boats/blog/poll-progress
    fn poll_progress(&mut self, cx: &mut Context<'_>) {
        while let Ready(Some(maybe_output)) = self.futures.poll_next(cx) {
            // Handle the filter's `None` output.
            if let Some(output) = maybe_output {
                assert!(self.outputs.len() < self.outputs.capacity());
                self.outputs.push_back(output);
            }
        }
        // If the loop above ended in `Ready(None)`, then there are no buffered futures left. If it
        // ended in `Pending`, there are futures left, and a wakeup is registered.
    }

    fn has_output(&self) -> bool {
        !self.outputs.is_empty()
    }

    fn pop_output(&mut self) -> Option<U> {
        self.outputs.pop_front()
    }
}

enum InnerExecutorType {
    Ordered { limit: usize },
    Unordered { limit: usize },
}

fn new_inner_executor<Fut: Future<Output = Option<U>>, U>(
    executor_type: InnerExecutorType,
) -> InnerExecutor<Fut, U> {
    match executor_type {
        InnerExecutorType::Ordered { limit } => InnerExecutor {
            futures: OrderedOrUnorderedFutures::Ordered(FuturesOrdered::new()),
            outputs: VecDeque::with_capacity(limit),
        },
        InnerExecutorType::Unordered { limit } => InnerExecutor {
            futures: OrderedOrUnorderedFutures::Unordered(FuturesUnordered::new()),
            outputs: VecDeque::with_capacity(limit),
        },
    }
}

// We have to `await` this when we call it, but it always returns `Ready` immediately, so it's not
// e.g. a cancellation point.
async fn with_context_nonblocking<T>(f: impl FnOnce(&mut Context) -> T) -> T {
    // `poll_fn` demands a FnMut, so use an Option workaround.
    let mut f_option = Some(f);
    std::future::poll_fn(move |cx| {
        let t = (f_option.take().unwrap())(cx);
        Ready(t)
    })
    .await
}

// This is clearly an `async fn` that wishes it was a `Future` impl. It has to be this way to
// compile on stable today, because there's no way to refer to the future that an
// `AsyncFn`/`AsyncFnMut` returns, so we can't put it in a struct type (that also owns the closure
// and needs to be able to express the relationship), and we can't put a `Send` bound on it either
// (so we can't box it up in a way that's compatible with e.g. Tokio).
async fn concurrent_pipe_executor<T, U>(
    inner_executor_type: InnerExecutorType,
    mut inputs: Receiver<T>,
    filter_map: impl AsyncFn(T) -> Option<U>,
    mut outputs: Sender<U>,
) {
    let mut inner_executor = new_inner_executor(inner_executor_type);

    // If the input channel is closed, and the inner executor is empty (of both futures and
    // outputs), then we're done. Note that here we don't consider whether there's an output in
    // flight; that's handled by the outputs channel.
    while !inputs.is_terminated() || inner_executor.len() > 0 {
        // The input, output, and poll_progress tasks can all unblock each other, so we might need
        // to run this loop more than once before yielding `pending!()`. We'll only yield if *none*
        // of them made progress.
        let mut loop_again_before_yielding = false;

        // If an output is ready, and there's space in the outputs channel, send one.
        let mut output_in_flight = 0;
        let mut attempted_to_send_output = false;
        if inner_executor.has_output() {
            attempted_to_send_output = true;
            let outputs_ready = with_context_nonblocking(|cx| outputs.poll_ready(cx)).await;
            match outputs_ready {
                Pending => output_in_flight = 1, // A wakeup is scheduled for this.
                Ready(Err(_)) => panic!("outputs channel should not be closed"),
                Ready(Ok(())) => {
                    let output = inner_executor.pop_output().unwrap();
                    outputs.start_send(output).unwrap();
                    loop_again_before_yielding = true;
                }
            }
        }

        // If there's capacity in the inner executor (even counting the output in flight, if any)
        // try to receive an input.
        if inner_executor.len() + output_in_flight < inner_executor.capacity() {
            let next_input =
                with_context_nonblocking(|cx| Pin::new(&mut inputs).poll_next(cx)).await;
            match next_input {
                Pending => {}     // A wakeup is scheduled for this.
                Ready(None) => {} // The channel is closed.
                Ready(Some(input)) => {
                    let future = (filter_map)(input);
                    inner_executor.push(future);
                    loop_again_before_yielding = true;
                }
            }
        }

        // Drive any buffered futures, potentially including one we just received. This might make
        // progress internally, but the only way it could unblock the other tasks is by giving us
        // some output to send when we didn't have any before.
        with_context_nonblocking(|cx| inner_executor.poll_progress(cx)).await;
        if !attempted_to_send_output && inner_executor.has_output() {
            loop_again_before_yielding = true;
        }

        if !loop_again_before_yielding {
            // We've made as much progress as we can and scheduled all the relevant wakeups.
            futures::pending!();
        }
    }
}

pub fn pipeline<S: Stream>(stream: S) -> AsyncPipeline<impl Future, S::Item> {
    let (mut sender, receiver) = channel(0);
    AsyncPipeline {
        future: async move {
            let mut stream = pin!(stream);
            while let Some(item) = stream.next().await {
                sender.send(item).await.expect("channel should not close");
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
    pub fn map<F, U>(self, mut f: F) -> AsyncPipeline<impl Future, U>
    where
        F: AsyncFnMut(T) -> U,
    {
        self.filter_map(async move |t| Some(f(t).await))
    }

    pub fn map_concurrent<F, U>(self, f: F, limit: usize) -> AsyncPipeline<impl Future, U>
    where
        F: AsyncFn(T) -> U,
    {
        self.filter_map_concurrent(async move |t| Some(f(t).await), limit)
    }

    pub fn map_unordered<F, U>(self, f: F, limit: usize) -> AsyncPipeline<impl Future, U>
    where
        F: AsyncFn(T) -> U,
    {
        self.filter_map_unordered(async move |t| Some(f(t).await), limit)
    }

    pub fn filter<F>(self, mut f: F) -> AsyncPipeline<impl Future, T>
    where
        F: AsyncFnMut(&T) -> bool,
    {
        self.filter_map(async move |t| f(&t).await.then_some(t))
    }

    pub fn filter_concurrent<F>(self, f: F, limit: usize) -> AsyncPipeline<impl Future, T>
    where
        F: AsyncFn(&T) -> bool,
    {
        self.filter_map_concurrent(async move |t| f(&t).await.then_some(t), limit)
    }

    pub fn filter_unordered<F>(self, f: F, limit: usize) -> AsyncPipeline<impl Future, T>
    where
        F: AsyncFn(&T) -> bool,
    {
        self.filter_map_unordered(async move |t| f(&t).await.then_some(t), limit)
    }

    pub fn filter_map<F, U>(mut self, mut f: F) -> AsyncPipeline<impl Future, U>
    where
        F: AsyncFnMut(T) -> Option<U>,
    {
        let (mut sender, receiver) = channel(0);
        AsyncPipeline {
            future: join(self.future, async move {
                while let Some(input) = self.outputs.next().await {
                    if let Some(output) = f(input).await {
                        sender.send(output).await.expect("channel should not close");
                    }
                }
            }),
            outputs: receiver,
        }
    }

    pub fn filter_map_concurrent<F, U>(self, f: F, limit: usize) -> AsyncPipeline<impl Future, U>
    where
        F: AsyncFn(T) -> Option<U>,
    {
        let (sender, receiver) = channel(0);
        AsyncPipeline {
            future: join(
                self.future,
                concurrent_pipe_executor(
                    InnerExecutorType::Ordered { limit },
                    self.outputs,
                    async move |t| f(t).await,
                    sender,
                ),
            ),
            outputs: receiver,
        }
    }

    pub fn filter_map_unordered<F, U>(self, f: F, limit: usize) -> AsyncPipeline<impl Future, U>
    where
        F: AsyncFn(T) -> Option<U>,
    {
        let (sender, receiver) = channel(0);
        AsyncPipeline {
            future: join(
                self.future,
                concurrent_pipe_executor(
                    InnerExecutorType::Unordered { limit },
                    self.outputs,
                    async move |t| f(t).await,
                    sender,
                ),
            ),
            outputs: receiver,
        }
    }

    /// By default a single element is buffered in between each pipeline stage. This adds a
    /// pipeline state that buffers extra elements.
    pub fn buffered(mut self, buf_size: usize) -> AsyncPipeline<impl Future, T> {
        assert!(buf_size > 0, "`buf_size` must be at least 1");
        let (mut sender, receiver) = channel(buf_size);
        AsyncPipeline {
            future: join(self.future, async move {
                while let Some(input) = self.outputs.next().await {
                    sender.send(input).await.expect("channel should not close");
                }
            }),
            outputs: receiver,
        }
    }

    pub async fn for_each<F>(mut self, mut f: F)
    where
        F: AsyncFnMut(T),
    {
        join(self.future, async move {
            while let Some(input) = self.outputs.next().await {
                f(input).await;
            }
        })
        .await;
    }

    pub async fn for_each_concurrent<F>(self, f: F, limit: usize)
    where
        F: AsyncFn(T),
    {
        self.map_unordered(
            async move |input| {
                f(input).await;
            },
            limit,
        )
        .for_each(async |_| {})
        .await;
    }

    pub async fn collect<C: Default + Extend<T>>(mut self) -> C {
        join(self.future, async move {
            let mut collection = C::default();
            while let Some(input) = self.outputs.next().await {
                collection.extend(std::iter::once(input));
            }
            collection
        })
        .await
        .1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Mutex;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_for_each() {
        let mut v = Vec::new();
        pipeline(futures::stream::iter(0..5))
            .map(async |x| {
                sleep(Duration::from_millis(1)).await;
                x + 1
            })
            .map(async |x| {
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
            .map(async |x| {
                sleep(Duration::from_millis(1)).await;
                x + 1
            })
            .map(async |x| {
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
            .map_concurrent(
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

    #[tokio::test]
    async fn test_concurrent_unordered() {
        use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
        static FUTURES_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        static MAX_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        let v: Vec<i32> = pipeline(futures::stream::iter(0..10))
            .map_unordered(
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

    #[tokio::test]
    async fn test_filter() {
        let v = Mutex::new(Vec::new());
        pipeline(futures::stream::iter(0..12))
            .filter(async |x| x % 4 != 0)
            .filter_concurrent(async |x| x % 4 != 1, 3)
            .filter_unordered(
                async |x| {
                    // Provoke out-of-order behavior.
                    sleep(Duration::from_millis(12 - x)).await;
                    x % 4 != 2
                },
                3,
            )
            .for_each_concurrent(
                async |x| {
                    v.lock().await.push(x);
                },
                3,
            )
            .await;
        let mut v = v.lock().await;
        // Assert out-of-order behavior. This might end up being flaky, and if so I'll remove it.
        assert_ne!(*v, vec![3, 7, 11]);
        v.sort();
        assert_eq!(*v, vec![3, 7, 11]);
    }
}
