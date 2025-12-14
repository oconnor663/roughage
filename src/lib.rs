//! This crate provides a single type, `AsyncPipeline`, which is an alternative to [`buffered`]
//! streams, [`FuturesOrdered`], and [`FuturesUnordered`].
//!
//! All of those are prone to deadlocks if any of their buffered/concurrent futures touches an
//! async lock of any kind, _even indirectly_. (For example, note that [`tokio::sync::mpsc`]
//! channels [use a `Semaphore` internally][internally].) The problem is that they don't
//! consistently poll their buffered futures, so a future holding a lock could stop making forward
//! progress through no fault of its own. `AsyncPipeline` fixes this whole class of deadlocks by
//! consistently polling all its in-flight futures until they complete. In other words,
//! `AsyncPipeline` will never "snooze" a future.
//!
//! [`buffered`]: https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.buffered
//! [`FuturesOrdered`]: https://docs.rs/futures/latest/futures/stream/struct.FuturesOrdered.html
//! [`FuturesUnordered`]: https://docs.rs/futures/latest/futures/stream/struct.FuturesUnordered.html
//! [`tokio::sync::mpsc`]: https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html
//! [internally]: https://github.com/tokio-rs/tokio/blob/0ec0a8546105b9f250f868b77e42c82809703aab/tokio/src/sync/mpsc/bounded.rs#L162
//!
//! Here's how easy it is to provoke a deadlock with `buffered` streams:
//!
//! ```rust,no_run
//! use futures::StreamExt;
//! use tokio::sync::Mutex;
//! use tokio::time::{Duration, sleep};
//!
//! static LOCK: Mutex<()> = Mutex::const_new(());
//!
//! // An innocent example function that touches an async lock. Note that the deadlocks below can
//! // happen even if this function is buried three crates deep in some dependency you never see.
//! async fn foo() {
//!     let _guard = LOCK.lock().await;
//!     sleep(Duration::from_millis(1)).await;
//!     // It *looks like* `foo` is guaranteed to release this lock after 1 ms...
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! futures::stream::iter([foo(), foo()])
//!     .buffered(2)
//!     .for_each(|_| async {
//!         foo().await; // Deadlock!
//!     })
//!     .await;
//! # }
//! ```
//!
//! Here's the same deadlock with `FuturesUnordered`:
//!
//! ```rust,no_run
//! # use futures::StreamExt;
//! # use tokio::sync::Mutex;
//! # use tokio::time::{Duration, sleep};
//! # static LOCK: Mutex<()> = Mutex::const_new(());
//! # async fn foo() {
//! #     let _guard = LOCK.lock().await;
//! #     sleep(Duration::from_millis(1)).await;
//! # }
//! # #[tokio::main]
//! # async fn main() {
//! let mut unordered = futures::stream::FuturesUnordered::new();
//! unordered.push(foo());
//! unordered.push(foo());
//! while let Some(_) = unordered.next().await {
//!     foo().await; // Deadlock!
//! }
//! # }
//! ```
//!
//! An `AsyncPipeline` does not have this problem, because once it's started a future internally,
//! it never stops polling it:
//!
//! ```rust
//! # use tokio::sync::Mutex;
//! # use tokio::time::{Duration, sleep};
//! # static LOCK: Mutex<()> = Mutex::const_new(());
//! # async fn foo() {
//! #     let _guard = LOCK.lock().await;
//! #     sleep(Duration::from_millis(1)).await;
//! # }
//! # #[tokio::main]
//! # async fn main() {
//! use roughage::AsyncPipeline;
//!
//! AsyncPipeline::from_iter(0..100)
//!     .map_concurrent(|_| foo(), 10)
//!     .map_unordered(|_| foo(), 10)
//!     .for_each_concurrent(|_| foo(), 10)
//!     .await;
//! // Deadlock free!
//! # }
//! ```
//!
//! "Roughage" (_ruff_-_edge_) is an older term for dietary fiber. It keeps our pipes running
//! smoothly.

use atomic_refcell::AtomicRefCell;
use futures::future::{Fuse, FusedFuture, join};
use futures::stream::{FusedStream, FuturesOrdered, FuturesUnordered};
use futures::{FutureExt, Stream, StreamExt};
use std::collections::VecDeque;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::Waker;
use std::task::{Context, Poll, Poll::Pending, Poll::Ready};

#[derive(Debug)]
struct BufferInner<T> {
    items: VecDeque<T>,
    is_closed: bool,
    // This waker is only used in the backwards direction, when a later stage wakes up a prior one
    // by clearing space in its output buffer. Wakers aren't needed in the forward direction,
    // because we always poll each stage right after the one that might've generated input for it.
    sender_waker: Option<Waker>,
}

#[derive(Debug)]
struct Buffer<T>(Arc<AtomicRefCell<BufferInner<T>>>);

impl<T> Buffer<T> {
    fn new(capacity: usize) -> Self {
        Self(Arc::new(AtomicRefCell::new(BufferInner {
            items: VecDeque::with_capacity(capacity),
            is_closed: false,
            sender_waker: None,
        })))
    }

    fn push(&self, item: T) {
        let mut this = self.0.borrow_mut();
        assert!(this.items.len() < this.items.capacity());
        assert!(!this.is_closed);
        this.items.push_back(item);
    }
}

impl<T> Clone for Buffer<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

trait TypeErasedBuffer {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn close(&self);
    fn register_sender_waker(&self, waker: Waker);
}

impl<T> TypeErasedBuffer for Buffer<T> {
    fn len(&self) -> usize {
        self.0.borrow().items.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn close(&self) {
        self.0.borrow_mut().is_closed = true;
    }

    fn register_sender_waker(&self, waker: Waker) {
        let mut this = self.0.borrow_mut();
        assert!(!this.items.is_empty());
        this.sender_waker = Some(waker);
    }
}

impl<T> Stream for Buffer<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.0.borrow_mut();
        // Any read of the buffer could potentially unblock the sender, even if the buffer isn't
        // full, because the sender is also counting their own futures in flight against the
        // capacity.
        if let Some(waker) = this.sender_waker.take() {
            // However, the sender shouldn't register a waker if the buffer is *empty*.
            assert!(!this.items.is_empty());
            waker.wake();
        }
        if let Some(item) = this.items.pop_front() {
            Ready(Some(item))
        } else if this.is_closed {
            Ready(None)
        } else {
            // NOTE: No wakeup is registered in this case. The pipeline itself tracks when
            // re-filling an empty buffer (or draining a full one) leads to a re-poll.
            Pending
        }
    }
}

impl<T> FusedStream for Buffer<T> {
    fn is_terminated(&self) -> bool {
        let this = self.0.borrow();
        this.items.is_empty() && this.is_closed
    }
}

pin_project_lite::pin_project! {
    struct PipelineStage<Fut, T> {
        #[pin]
        future: Fuse<Fut>,
        outputs: Buffer<T>,
    }
}

trait TypeErasedStage {
    fn future(self: Pin<&mut Self>) -> Pin<&mut dyn Future<Output = ()>>;
    fn is_done(&self) -> bool;
    fn outputs_buffer(&self) -> &dyn TypeErasedBuffer;
}

impl<Fut: Future<Output = ()>, T> TypeErasedStage for PipelineStage<Fut, T> {
    fn future(self: Pin<&mut Self>) -> Pin<&mut dyn Future<Output = ()>> {
        self.project().future
    }

    fn is_done(&self) -> bool {
        self.future.is_terminated()
    }

    fn outputs_buffer(&self) -> &dyn TypeErasedBuffer {
        &self.outputs
    }
}

fn poll_stages<'a>(
    mut stages: Vec<Pin<Box<dyn TypeErasedStage + 'a>>>,
) -> impl Future<Output = ()> + 'a {
    std::future::poll_fn(move |cx| {
        if stages.is_empty() {
            return Ready(());
        }
        for i in 0..stages.len() {
            let (prev_slice, rest_slice) = stages.split_at_mut(i);
            let previous_stage = prev_slice.last().map(|s| &**s);
            let inputs = previous_stage.map(TypeErasedStage::outputs_buffer);
            let current_stage = &mut rest_slice[0];
            if current_stage.as_mut().future().poll(cx).is_ready() {
                assert!(
                    previous_stage.is_none_or(TypeErasedStage::is_done),
                    "later stage ({}) finished before previous ({})",
                    i,
                    i - 1,
                );
                assert!(
                    inputs.is_none_or(TypeErasedBuffer::is_empty),
                    "stage finished with leftover inputs"
                );
            }
        }
        if stages.last().unwrap().is_done() {
            Ready(())
        } else {
            Pending
        }
    })
}

enum Executor<Fut: Future> {
    Ordered(FuturesOrdered<Fut>),
    Unordered(FuturesUnordered<Fut>),
}

enum ExecutorKind {
    Ordered,
    Unordered,
}

impl<Fut: Future> Executor<Fut> {
    fn new(kind: ExecutorKind) -> Self {
        match kind {
            ExecutorKind::Ordered => Self::Ordered(FuturesOrdered::new()),
            ExecutorKind::Unordered => Self::Unordered(FuturesUnordered::new()),
        }
    }

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

fn filter_map<T, U, S, F, Fut>(
    mut inputs: Pin<&mut S>,
    mut f: F,
    outputs: Buffer<U>,
    limit: usize,
    kind: ExecutorKind,
) -> impl Future<Output = ()>
where
    S: Stream<Item = T> + FusedStream,
    F: FnMut(T) -> Fut,
    Fut: Future<Output = Option<U>>,
{
    let mut executor = Executor::new(kind);
    std::future::poll_fn(move |cx| {
        loop {
            let mut keep_looping = false;
            // Try to receive an input input, if there's enough space in the output buffer. When
            // the output buffer is full, we shouldn't have any futures in flight, and we won't
            // start any until the next stage consumes an input. We require that the stream is
            // fused, so we can keep polling it even after it's finished.
            if executor.len() + outputs.len() < limit {
                // If no input is available, this *might* register a wakeup, but only if the caller
                // has added async work to the input stream. The `Buffer` itself will never
                // register a wakeup, and instead the pipeline needs to track when one stage
                // might've unblocked another.
                if let Ready(Some(input)) = inputs.as_mut().poll_next(cx) {
                    executor.push(f(input));
                    keep_looping = true;
                }
            } else if outputs.len() > 0 {
                // If there wasn't enough space in the output buffer, register a "sender waker".
                // Note that the buffer doesn't need to be full for this to matter (just
                // non-empty), because we're counting our futures in flight against the capacity,
                // and it can't see that.
                outputs.register_sender_waker(cx.waker().clone());
            }
            // Drive the futures in flight. If some of them are still pending, this will register a
            // wakeup.
            if let Ready(Some(maybe_output)) = executor.poll_next(cx) {
                // The closure is a filter map, so we have another layer of `Option` here, and we drop
                // the `None`s.
                if let Some(output) = maybe_output {
                    outputs.push(output);
                }
                keep_looping = true;
            }
            // If either the input side or the executor side retuned `Ready(Some(_))` above, keep
            // looping.
            if !keep_looping {
                break;
            }
        }
        if inputs.is_terminated() && executor.len() == 0 {
            outputs.close();
            Ready(())
        } else {
            Pending
        }
    })
}

/// Like a [`buffered`] stream, with the added guarantee that it won't "snooze" futures.
///
/// See the [crate-level documentation](crate) for an example.
///
/// [`buffered`]: https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.buffered
pub struct AsyncPipeline<'a, S: Stream + 'a> {
    outputs: S,
    stages: Vec<Pin<Box<dyn TypeErasedStage + 'a>>>,
}

impl<'a, I: Iterator> AsyncPipeline<'a, futures::stream::Iter<I>> {
    pub fn from_iter(iter: impl IntoIterator<IntoIter = I>) -> Self {
        Self::from_stream(futures::stream::iter(iter))
    }
}

impl<'a, S: Stream> AsyncPipeline<'a, S> {
    pub fn from_stream(stream: S) -> Self {
        Self {
            outputs: stream,
            stages: Vec::new(),
        }
    }

    pub async fn for_each(self, mut f: impl AsyncFnMut(S::Item)) {
        join(poll_stages(self.stages), async {
            let mut outputs = pin!(self.outputs);
            while let Some(item) = outputs.next().await {
                f(item).await;
            }
        })
        .await;
    }

    pub async fn for_each_concurrent(self, f: impl AsyncFn(S::Item), limit: usize) {
        let mut inputs = pin!(self.outputs.fuse());
        let mut executor = FuturesUnordered::new();
        join(poll_stages(self.stages), async {
            // This loop is basically a copy of `filter_map`, except that here we can use
            // `AsyncFn()` here instead of `FnMut() -> impl Future`. This requires the "async fn
            // that wishes it was a Future impl" programming style. When return type notation is
            // stable, we should be able to use `AsyncFn` throughout the API and unify these.
            loop {
                let mut keep_looping = false;
                if executor.len() < limit
                    && let Ready(Some(input)) = futures::poll!(inputs.next())
                {
                    executor.push(f(input));
                    keep_looping = true;
                }
                if let Ready(Some(())) = futures::poll!(executor.next()) {
                    keep_looping = true;
                }
                if keep_looping {
                    continue;
                } else if inputs.is_terminated() && executor.is_empty() {
                    return;
                } else {
                    futures::pending!();
                }
            }
        })
        .await;
    }

    pub async fn collect<C: Default + Extend<S::Item>>(self) -> C {
        let mut collection = C::default();
        self.for_each(async |item| {
            collection.extend(std::iter::once(item));
        })
        .await;
        collection
    }

    pub fn adapt_output_stream<F, S2>(self, f: F) -> AsyncPipeline<'a, S2>
    where
        F: FnOnce(S) -> S2,
        S2: Stream,
    {
        AsyncPipeline {
            outputs: f(self.outputs),
            stages: self.stages,
        }
    }

    pub fn map_concurrent<F, Fut, U>(
        self,
        mut f: F,
        limit: usize,
    ) -> AsyncPipeline<'a, impl Stream<Item = U>>
    where
        F: FnMut(S::Item) -> Fut + 'a,
        Fut: Future<Output = U> + 'a,
        U: 'a,
    {
        self.filter_map_concurrent(
            move |item| {
                let fut = f(item);
                async { Some(fut.await) }
            },
            limit,
        )
    }

    pub fn map_unordered<F, Fut, U>(
        self,
        mut f: F,
        limit: usize,
    ) -> AsyncPipeline<'a, impl Stream<Item = U>>
    where
        F: FnMut(S::Item) -> Fut + 'a,
        Fut: Future<Output = U> + 'a,
        U: 'a,
    {
        self.filter_map_unordered(
            move |item| {
                let fut = f(item);
                async { Some(fut.await) }
            },
            limit,
        )
    }

    fn filter_map_inner<F, Fut, U>(
        mut self,
        f: F,
        limit: usize,
        kind: ExecutorKind,
    ) -> AsyncPipeline<'a, impl Stream<Item = U>>
    where
        F: FnMut(S::Item) -> Fut + 'a,
        Fut: Future<Output = Option<U>> + 'a,
        U: 'a,
    {
        let buffer = Buffer::<U>::new(limit);
        let buffer_clone = buffer.clone();
        self.stages.push(Box::pin(PipelineStage {
            outputs: buffer.clone(),
            future: async move {
                let inputs = pin!(self.outputs.fuse());
                filter_map(inputs, f, buffer_clone, limit, kind).await;
            }
            .fuse(),
        }));
        AsyncPipeline {
            outputs: buffer,
            stages: self.stages,
        }
    }

    pub fn filter_map_concurrent<F, Fut, U>(
        self,
        f: F,
        limit: usize,
    ) -> AsyncPipeline<'a, impl Stream<Item = U>>
    where
        F: FnMut(S::Item) -> Fut + 'a,
        Fut: Future<Output = Option<U>> + 'a,
        U: 'a,
    {
        self.filter_map_inner(f, limit, ExecutorKind::Ordered)
    }

    pub fn filter_map_unordered<F, Fut, U>(
        self,
        f: F,
        limit: usize,
    ) -> AsyncPipeline<'a, impl Stream<Item = U>>
    where
        F: FnMut(S::Item) -> Fut + 'a,
        Fut: Future<Output = Option<U>> + 'a,
        U: 'a,
    {
        self.filter_map_inner(f, limit, ExecutorKind::Unordered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
    use tokio::sync::Mutex;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::{Duration, sleep};
    use tokio_stream::wrappers::UnboundedReceiverStream;

    #[tokio::test]
    async fn test_for_each() {
        let inputs = [0, 1, 2, 3, 4];
        let mut v = Vec::new();
        // Iterate over references, to make sure we can.
        AsyncPipeline::from_iter(&inputs)
            .adapt_output_stream(|s| {
                s.then(async |x| {
                    sleep(Duration::from_millis(1)).await;
                    x + 1
                })
            })
            .map_concurrent(
                async |x| {
                    sleep(Duration::from_millis(1)).await;
                    10 * x
                },
                3,
            )
            .for_each(async |x| {
                v.push(x);
            })
            .await;
        assert_eq!(v, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test]
    async fn test_for_each_concurrent() {
        let v = Mutex::new(Vec::new());
        AsyncPipeline::from_iter(0..5)
            .adapt_output_stream(|s| {
                s.then(async |x| {
                    sleep(Duration::from_millis(1)).await;
                    x + 1
                })
            })
            .map_concurrent(
                async |x| {
                    sleep(Duration::from_millis(1)).await;
                    10 * x
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
        assert_eq!(v.into_inner(), vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test]
    async fn test_collect() {
        let v: Vec<_> = AsyncPipeline::from_iter(0..5)
            .adapt_output_stream(|s| {
                s.then(async |x| {
                    sleep(Duration::from_millis(1)).await;
                    x + 1
                })
                .then(async |x| {
                    sleep(Duration::from_millis(1)).await;
                    10 * x
                })
            })
            .collect()
            .await;
        assert_eq!(v, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test]
    async fn test_max_in_flight() {
        static ELEMENTS_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        let mut i = 0;
        AsyncPipeline::from_iter(std::iter::from_fn(|| {
            if i < 10 {
                let in_flight = ELEMENTS_IN_FLIGHT.fetch_add(1, Relaxed);
                assert_eq!(in_flight, 0, "too many elements in flight at i = {i}");
                i += 1;
                Some(i)
            } else {
                None
            }
        }))
        .for_each(async |i| {
            let in_flight = ELEMENTS_IN_FLIGHT.fetch_sub(1, Relaxed);
            assert_eq!(in_flight, 1, "too many elements in flight at i = {i}");
            sleep(Duration::from_millis(1)).await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_map_concurrent() {
        use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
        static FUTURES_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        static MAX_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        let v: Vec<i32> = AsyncPipeline::from_iter(0..10)
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
        assert_eq!(MAX_IN_FLIGHT.load(Relaxed), 3);
    }

    #[tokio::test]
    async fn test_map_unordered() {
        use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
        static FUTURES_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        static MAX_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
        let v: Vec<i32> = AsyncPipeline::from_iter(0..10)
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
        assert_eq!(MAX_IN_FLIGHT.load(Relaxed), 3);
    }

    #[tokio::test]
    async fn test_deadlocks() {
        async fn foo(i: i32) -> i32 {
            static LOCK: Mutex<()> = Mutex::const_new(());
            println!("locking foo({i})");
            let _guard = LOCK.lock();
            println!("sleeping foo({i})");
            sleep(Duration::from_millis(rand::random_range(0..10))).await;
            println!("waking foo({i})");
            i + 1
        }

        let v = Mutex::new(Vec::new());
        AsyncPipeline::from_iter(0..100)
            .map_concurrent(async |i| foo(i).await, 10)
            .map_unordered(async |i| foo(i).await, 10)
            .filter_map_concurrent(async |i| Some(foo(i).await), 10)
            .filter_map_unordered(async |i| Some(foo(i).await), 10)
            .for_each_concurrent(
                async |i| {
                    futures::join!(foo(i), foo(i), foo(i), foo(i), foo(i));
                    v.lock().await.push(foo(i).await);
                },
                10,
            )
            .await;

        let mut v = v.into_inner();
        v.sort();
        assert_eq!(v[..], (5..105).collect::<Vec<_>>());
    }

    /// The stream that the pipeline starts with can be a channel. This lets you "spawn" jobs into
    /// the pipeline, kind of like you would with a `FuturesUnordered`.
    #[tokio::test]
    async fn test_channel() {
        let lock = Mutex::new(());
        let atomic1 = AtomicU32::new(0);
        let atomic2 = AtomicU32::new(0);
        let num_jobs: usize = 1000;
        let (sender, receiver) = unbounded_channel::<()>();
        let pipeline = AsyncPipeline::from_stream(UnboundedReceiverStream::new(receiver))
            .for_each_concurrent(
                async |_| {
                    atomic1.fetch_add(1, Relaxed);
                    let _guard = lock.lock().await;
                    atomic2.fetch_add(1, Relaxed);
                },
                num_jobs,
            );
        join(pipeline, async {
            // Take the lock.
            let _guard = lock.lock().await;
            // Spawn a bunch of tasks in the pipeline. They will each increment `atomic1` and then
            // block on the lock.
            for _ in 0..num_jobs {
                sender.send(()).unwrap();
            }
            // Wait for `atomic1` to reach `num_jobs`. We do need to yield to let the pipeline make
            // progress.
            while atomic1.load(Relaxed) != num_jobs as u32 {
                sleep(Duration::from_millis(1)).await;
            }
            // At this point, none of the jobs have gotten to `atomic2`. It should still be zero.
            assert_eq!(atomic2.load(Relaxed), 0);
            // Unblock them.
            drop(_guard);
            // Wait for `atomic2` to reach `num_jobs` also.
            while atomic2.load(Relaxed) != num_jobs as u32 {
                sleep(Duration::from_millis(1)).await;
            }
            // Success! Drop the channel sender so that the pipeline can finish. (Making this block
            // `async move` would automatically drop the sender here, but it would make it annoying
            // to work with the other shared values.)
            drop(sender);
        })
        .await;
    }
}
