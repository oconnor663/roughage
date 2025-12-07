use futures::channel::mpsc::{Receiver, Sender, channel};
use futures::future::{MaybeDone, maybe_done};
use futures::stream::{FusedStream, FuturesOrdered, FuturesUnordered};
use futures::{FutureExt, SinkExt, Stream, StreamExt, join};
use std::collections::VecDeque;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Poll::Pending, Poll::Ready};

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

    fn wait_for_space(&mut self) -> impl Future<Output = ()> {
        std::future::poll_fn(|cx| match self.sender.poll_ready(cx) {
            Ready(Ok(())) => Ready(()),
            Ready(Err(_)) => {
                panic!("this channel should never close");
            }
            Pending => Pending,
        })
    }
}

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
enum InnerExecutor<Fut: Future<Output = Option<U>>, U> {
    Serial(Pin<Box<MaybeDone<Fut>>>),
    Buffered {
        futures: OrderedOrUnorderedFutures<Fut>,
        outputs: VecDeque<U>,
    },
}

impl<Fut: Future<Output = Option<U>>, U> InnerExecutor<Fut, U> {
    fn new_serial() -> Self {
        Self::Serial(Box::pin(MaybeDone::Gone))
    }

    fn new_ordered(limit: usize) -> Self {
        Self::Buffered {
            futures: OrderedOrUnorderedFutures::Ordered(FuturesOrdered::new()),
            outputs: VecDeque::with_capacity(limit),
        }
    }

    fn new_unordered(limit: usize) -> Self {
        Self::Buffered {
            futures: OrderedOrUnorderedFutures::Unordered(FuturesUnordered::new()),
            outputs: VecDeque::with_capacity(limit),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Serial(maybe_done) => match &**maybe_done {
                MaybeDone::Future(_) | MaybeDone::Done(Some(_)) => 1,
                // NOTE: A `filter_map` future that returns `None` is considered not to have
                // returned any output at all.
                MaybeDone::Done(None) | MaybeDone::Gone => 0,
            },
            Self::Buffered { futures, outputs } => futures.len() + outputs.len(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Serial(_) => 1,
            Self::Buffered { outputs, .. } => outputs.capacity(),
        }
    }

    fn is_full(&self) -> bool {
        assert!(self.len() <= self.capacity());
        self.len() == self.capacity()
    }

    fn push(&mut self, future: Fut) {
        assert!(!self.is_full());
        match self {
            Self::Serial(maybe) => {
                maybe.set(MaybeDone::Future(future));
            }
            Self::Buffered { futures, .. } => {
                futures.push(future);
            }
        }
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
        match self {
            Self::Serial(maybe) => {
                // Polling `MaybeDone::Gone` panics, so we need this check.
                if matches!(**maybe, MaybeDone::Future(_)) {
                    _ = maybe.as_mut().poll(cx);
                    // Note that `MaybeDone::Done(None)` is considered equivalent to
                    // `MaybeDone::Gone` in `.len()` and `.has_output()`, and it's flattened in
                    // `.pop_output()`, so we don't need to do anything with it here.
                }
            }
            Self::Buffered { futures, outputs } => {
                while let Ready(Some(maybe_output)) = futures.poll_next(cx) {
                    // Handle the filter's `None` output.
                    if let Some(output) = maybe_output {
                        assert!(outputs.len() < outputs.capacity());
                        outputs.push_back(output);
                    }
                }
                // If the loop above ended in `Ready(None)`, then there are no buffered futures
                // left. If it ended in `Pending`, there are futures left, and a wakeup is
                // registered.
            }
        }
    }

    fn has_output(&self) -> bool {
        match self {
            // NOTE: A `filter_map` future that returns `None` is considered not to have returned
            // any output at all.
            Self::Serial(maybe) => matches!(**maybe, MaybeDone::Done(Some(_))),
            Self::Buffered { outputs, .. } => !outputs.is_empty(),
        }
    }

    fn pop_output(&mut self) -> Option<U> {
        match self {
            // NOTE: A `filter_map` future that returns `None` is considered not to have returned
            // any output at all.
            Self::Serial(maybe) => maybe.as_mut().take_output().flatten(),
            Self::Buffered { outputs, .. } => outputs.pop_front(),
        }
    }
}

struct PipeExecutor<T, F, Fut, U>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = Option<U>>,
{
    receiver: Receiver<T>,
    filter_map: F,
    executor: InnerExecutor<Fut, U>,
    sender: Sender<U>,
}

impl<T, F, Fut, U> Unpin for PipeExecutor<T, F, Fut, U>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = Option<U>>,
{
}

fn serial_executor<T, F, Fut, U>(
    receiver: Receiver<T>,
    filter_map: F,
    sender: Sender<U>,
) -> PipeExecutor<T, F, Fut, U>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = Option<U>>,
{
    PipeExecutor {
        receiver,
        filter_map,
        executor: InnerExecutor::new_serial(),
        sender,
    }
}

fn ordered_executor<T, F, Fut, U>(
    receiver: Receiver<T>,
    filter_map: F,
    limit: usize,
    sender: Sender<U>,
) -> PipeExecutor<T, F, Fut, U>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = Option<U>>,
{
    PipeExecutor {
        receiver,
        filter_map,
        executor: InnerExecutor::new_ordered(limit),
        sender,
    }
}

fn unordered_executor<T, F, Fut, U>(
    receiver: Receiver<T>,
    filter_map: F,
    limit: usize,
    sender: Sender<U>,
) -> PipeExecutor<T, F, Fut, U>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = Option<U>>,
{
    PipeExecutor {
        receiver,
        filter_map,
        executor: InnerExecutor::new_unordered(limit),
        sender,
    }
}

impl<T, F, Fut, U> Future for PipeExecutor<T, F, Fut, U>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = Option<U>>,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        // If an output is ready, and there's space in the outputs channel, send one.
        let mut output_in_flight = 0;
        if self.executor.has_output() {
            match self.sender.poll_ready(cx) {
                Pending => output_in_flight = 1, // A wakeup is scheduled for this.
                Ready(Err(_)) => panic!("outputs channel should not be closed"),
                Ready(Ok(())) => {
                    let output = self.executor.pop_output().unwrap();
                    self.sender.start_send(output).unwrap();
                }
            }
        }

        // If there's capacity in the inner executor (even counting the output in flight, if any)
        // try to receive an input.
        if self.executor.len() + output_in_flight < self.executor.capacity() {
            match Pin::new(&mut self.receiver).poll_next(cx) {
                Pending => {}     // A wakeup is scheduled for this.
                Ready(None) => {} // The channel is closed.
                Ready(Some(input)) => {
                    let future = (self.filter_map)(input);
                    self.executor.push(future);
                }
            }
        }

        // Drive any buffered futures, potentially including one we just received.
        self.executor.poll_progress(cx);

        // If the input channel is closed, and the inner executor is empty (of both futures and
        // outputs), then we're done. Otherwise, something above has registered a wakeup. Note that
        // here we don't consider whether there's an output in flight; that's handled by the
        // outputs channel. (We didn't register for a wakeup from that channel unless the executor
        // had outputs ready.)
        if self.receiver.is_terminated() && self.executor.len() == 0 {
            Ready(())
        } else {
            Pending
        }
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

    /// Drive all the contained futures until the first one (in order) is ready, then remove it and
    /// send its output to the provided channel. Or if this container is empty, return immediately.
    ///
    /// This method is **cancel-safe**. In particular, it waits until there's space in the output
    /// channel before taking the first future's output.
    fn await_and_send_first_output(
        &mut self,
        sender: &mut Sender<Fut::Output>,
    ) -> impl Future<Output = ()> {
        std::future::poll_fn(|cx| {
            let mut futures_iter = self.futures.iter_mut();
            // If the collection is empty, short-circuit.
            let Some(first) = futures_iter.next() else {
                return Pending;
            };
            // If the first future isn't ready, we won't poll the channel, because there's no point
            // in letting it wake us up yet. In this case poll all the other `MaybeDone` futures
            // and short-circuit. (This is critical for concurrency. Also note that polling a
            // `MaybeDone` that's already done is a no-op.)
            if first.as_mut().poll(cx).is_pending() {
                for fut in futures_iter {
                    _ = fut.as_mut().poll(cx);
                }
                return Pending;
            }
            // If the channel isn't ready, then again we'll poll all the other futures once and
            // short-circuit. In this case the channel will eventually wake us up.
            if !sender.poll_ready(cx).is_ready() {
                for fut in futures_iter {
                    _ = fut.as_mut().poll(cx);
                }
                return Pending;
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
            Ready(())
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
    pub fn then<F, U>(self, mut f: F) -> AsyncPipeline<impl Future, U>
    where
        F: AsyncFnMut(T) -> U,
    {
        let (sender, receiver) = rendezvous_channel();
        AsyncPipeline {
            future: async {
                join! {
                    self.future,
                    serial_executor(
                        self.outputs,
                        |t| f(t).map(Some),
                        sender.sender,
                    ),
                };
            },
            outputs: receiver,
        }
    }

    // pub fn then_concurrent<F, U>(self, f: F, limit: usize) -> AsyncPipeline<impl Future, U>
    // where
    //     F: AsyncFn(T) -> U,
    // {
    //     assert!(limit > 0, "`limit` must be at least 1");
    //     let (mut sender, receiver) = rendezvous_channel();
    //     AsyncPipeline {
    //         future: async move {
    //             join! {
    //                 self.future,
    //                 async {
    //                     let mut inputs = self.outputs;
    //                     let mut futures_ordered = FuturesOrdered::new();
    //                     let mut outputs = VecDeque::new();
    //                     loop {
    //                         let buffered_futures = futures_ordered.len();
    //                         let buffered_outputs = outputs.len();
    //                         let buffered = futures_ordered.len() + outputs.len();
    //                         assert!(buffered <= limit);
    //                         let at_capacity = buffered == limit;
    //                         // All three arms of this join need to be cancel-safe.
    //                         let (maybe_input, _, _) = join_me_maybe!(
    //                             // Cancel safety: the input is returned immediately without another
    //                             // await.
    //                             await_input: async {
    //                                 // If we're not at capacity, try to read another input.
    //                                 if !at_capacity && let Some(input) = inputs.next().await {
    //                                     // Once we get an input, cancel the other arms, so that we
    //                                     // can add this input to `futures_ordered`.
    //                                     await_output_space.cancel();
    //                                     poll_futures.cancel();
    //                                     Some(input)
    //                                 } else {
    //                                     None
    //                                 }
    //                             },
    //                             await_output_space: async {
    //                                 // If we have any buffered outputs, wait for space in the
    //                                 // output channel, and cancel the other arms when space opens
    //                                 // up.
    //                                 if buffered_outputs > 0 {
    //                                     sender.wait_for_space().await;
    //                                     await_input.cancel();
    //                                     poll_futures.cancel();
    //                                 }
    //                             },
    //                             // Cancel safety: the input is returned immediately without another
    //                             // await.
    //                             poll_futures: async {
    //                                 // Meanwhile, keep making forward progress on buffered futures.
    //                                 if let Some(output) = futures_ordered.next().await {
    //                                     outputs.push_back(output);
    //                                     await_input.cancel();
    //                                     await_output_space.cancel();
    //                                 }
    //                             },
    //                         );

    //                         let futures_len = futures.len();
    //                         let (maybe_input, _) = join_me_maybe!(
    //                             await_input: async {
    //                                 // If we're not at capacity, try to read another input.
    //                                 if futures_len < limit && let Some(input) = inputs.next().await {
    //                                     // We got another input. Cancel the send side so that we
    //                                     // can invoke the caller's closure and add the new future
    //                                     // to the collection immediately. Note that this side is
    //                                     // cancel-safe because it doesn't .await again.
    //                                     send_output.cancel();
    //                                     Some(input)
    //                                 } else {
    //                                     // Either we're at capacity or the inputs channel is
    //                                     // closed. Let the send side run.
    //                                     None
    //                                 }
    //                             },
    //                             send_output: async {
    //                                 // This method is cancel-safe, as documented above.
    //                                 futures.await_and_send_first_output(&mut sender.sender).await;
    //                                 // If there are still futures in the collection, cancel the
    //                                 // input side so that we can loop around. This restarts the
    //                                 // input side if we were at capacity before, and it lets this
    //                                 // side keep making progress on buffered futures.
    //                                 // TODO: Come up with a test case that fails if we miss this.
    //                                 if futures.len() > 0 {
    //                                     await_input.cancel();
    //                                 }
    //                                 // If not, let the input side run.
    //                             }
    //                         );
    //                         // If we got an input above, invoke the caller's closure and add the
    //                         // new future to our collection.
    //                         if let Some(input) = maybe_input.flatten() {
    //                             futures.push_back(f(input));
    //                         }
    //                         // Check whether this whole loop is done.
    //                         if inputs.is_done() && futures.len() == 0 {
    //                             return;
    //                         }
    //                     }
    //                 }
    //             };
    //         },
    //         outputs: receiver,
    //     }
    // }

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

    // #[tokio::test]
    // async fn test_concurrent() {
    //     use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
    //     static FUTURES_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
    //     static MAX_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);
    //     let v: Vec<i32> = pipeline(futures::stream::iter(0..10))
    //         .then_concurrent(
    //             async |i| {
    //                 let in_flight = FUTURES_IN_FLIGHT.fetch_add(1, Relaxed);
    //                 MAX_IN_FLIGHT.fetch_max(in_flight + 1, Relaxed);
    //                 sleep(Duration::from_millis(1)).await;
    //                 FUTURES_IN_FLIGHT.fetch_sub(1, Relaxed);
    //                 2 * i
    //             },
    //             3,
    //         )
    //         .collect()
    //         .await;
    //     assert_eq!(v, vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18]);
    // }
}
