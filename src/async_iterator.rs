use futures::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

#[must_use = "`AsyncIterator`s do nothing unless you `.for_each()` or `.collect()` them"]
pub trait AsyncIterator {
    type Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;

    // TODO: Should this return Poll<()> to indicate whether a wakeup is scheduled?
    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context<'_>);

    fn filter_map_concurrent<F, Fut, T>(
        self,
        f: F,
        limit: impl Into<Option<usize>>,
    ) -> FilterMap<Self, F, Fut, T>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = Option<T>>,
    {
        FilterMap {
            iter: Some(self),
            f,
            executor: crate::Executor::new(crate::ExecutorKind::Ordered),
            items: VecDeque::new(),
            limit: limit.into(),
        }
    }

    fn filter_map_unordered<F, Fut, T>(
        self,
        f: F,
        limit: impl Into<Option<usize>>,
    ) -> FilterMap<Self, F, Fut, T>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = Option<T>>,
    {
        FilterMap {
            iter: Some(self),
            f,
            executor: crate::Executor::new(crate::ExecutorKind::Unordered),
            items: VecDeque::new(),
            limit: limit.into(),
        }
    }

    fn buffered(
        self,
        limit: impl Into<Option<usize>>,
    ) -> impl AsyncIterator<Item = <Self::Item as Future>::Output>
    where
        Self: Sized,
        Self::Item: Future,
    {
        self.filter_map_concurrent(|fut| async move { Some(fut.await) }, limit)
    }

    fn buffer_unordered(
        self,
        limit: impl Into<Option<usize>>,
    ) -> impl AsyncIterator<Item = <Self::Item as Future>::Output>
    where
        Self: Sized,
        Self::Item: Future,
    {
        self.filter_map_unordered(|fut| async move { Some(fut.await) }, limit)
    }

    fn for_each_concurrent<F, Fut>(
        self,
        f: F,
        limit: impl Into<Option<usize>>,
    ) -> ForEach<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = ()>,
    {
        ForEach {
            iter: Some(self),
            f,
            // This combinator has no output, so it never needs to be ordered.
            executor: crate::Executor::new(crate::ExecutorKind::Unordered),
            limit: limit.into(),
        }
    }

    fn collect<C>(self) -> Collect<Self, C>
    where
        Self: Sized,
        C: Default + Extend<Self::Item>,
    {
        Collect {
            iter: self,
            items: Some(C::default()),
        }
    }
}

impl<S: Stream> AsyncIterator for S {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Stream::poll_next(self, cx)
    }

    fn poll_progress(self: Pin<&mut Self>, _: &mut Context<'_>) {}
}

pin_project_lite::pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct FilterMap<I, F, Fut, T>
    where
        I: AsyncIterator,
        F: FnMut(I::Item) -> Fut,
        Fut: Future<Output = Option<T>>,
    {
        #[pin]
        iter: Option<I>,
        f: F,
        executor: crate::Executor<Fut>,
        items: VecDeque<T>,
        limit: Option<usize>,
    }
}

impl<I, F, Fut, T> AsyncIterator for FilterMap<I, F, Fut, T>
where
    I: AsyncIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future<Output = Option<T>>,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().poll_progress(cx);
        let this = self.project();
        if let Some(item) = this.items.pop_front() {
            return Poll::Ready(Some(item));
        }
        if this.iter.is_some() || this.executor.len() > 0 {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context<'_>) {
        let mut this = self.project();
        loop {
            let mut progress = false;
            // If there's capacity, try to buffer a new future.
            let len = this.executor.len() + this.items.len();
            let has_capacity = this.limit.is_none_or(|limit| len < limit);
            if has_capacity && let Some(iter) = this.iter.as_mut().as_pin_mut() {
                match iter.poll_next(cx) {
                    Poll::Ready(Some(fut)) => {
                        this.executor.push((this.f)(fut));
                        progress = true;
                    }
                    Poll::Ready(None) => {
                        this.iter.set(None);
                    }
                    Poll::Pending => {}
                }
            }
            // Try to complete any buffered futures. Each future return either `Some(item)` or
            // `None`, so we have a doubly nested Option here. However, note that returning `None`
            // still counts as making progress, because it clears space in the buffer. Also note
            // that the executor returns `Ready(None)` when it's empty, but we never consider it
            // "finished". It's a unusual stream.
            if let Poll::Ready(Some(maybe_item)) = this.executor.poll_next(cx) {
                progress = true;
                if let Some(item) = maybe_item {
                    this.items.push_back(item);
                }
            }
            if !progress {
                break;
            }
        }
    }
}

pin_project_lite::pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct ForEach<I, F, Fut>
    where
        I: AsyncIterator,
        F: FnMut(I::Item) -> Fut,
        Fut: Future<Output = ()>,
    {
        #[pin]
        iter: Option<I>,
        f: F,
        executor: crate::Executor<Fut>,
        limit: Option<usize>,
    }
}

impl<I, F, Fut> Future for ForEach<I, F, Fut>
where
    I: AsyncIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut this = self.project();
        loop {
            let mut progress = false;
            // If there's capacity, try to buffer a new future.
            let has_capacity = this.limit.is_none_or(|limit| this.executor.len() < limit);
            if has_capacity && let Some(iter) = this.iter.as_mut().as_pin_mut() {
                match iter.poll_next(cx) {
                    Poll::Ready(Some(fut)) => {
                        this.executor.push((this.f)(fut));
                        progress = true;
                    }
                    Poll::Ready(None) => {
                        this.iter.set(None);
                    }
                    Poll::Pending => {}
                }
            }
            // Try to complete any buffered futures.
            if let Poll::Ready(Some(())) = this.executor.poll_next(cx) {
                progress = true;
            }
            if !progress {
                if this.iter.is_none() && this.executor.len() == 0 {
                    return Poll::Ready(());
                } else {
                    return Poll::Pending;
                }
            }
        }
    }
}

pin_project_lite::pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Collect<I, C>
    {
        #[pin]
        iter: I,
        items: Option<C>,
    }
}

impl<I, C> Future for Collect<I, C>
where
    I: AsyncIterator,
    C: Extend<I::Item>,
{
    type Output = C;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<C> {
        let mut this = self.project();
        let items = this.items.as_mut().expect("Collect polled after Ready");
        loop {
            match this.iter.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    items.extend(std::iter::once(item));
                }
                Poll::Ready(None) => {
                    let items = this.items.take().expect("Collect polled after Ready");
                    return Poll::Ready(items);
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncIterator;
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_for_each_concurrent() {
        // The "closure returning future" pattern doesn't support capturing mutable state. We will
        // eventually use AsyncFnMut instead, but currently we can only name its future in generic
        // bounds on nightly.
        let counter = AtomicU32::new(0);
        futures::stream::iter(0..100)
            .for_each_concurrent(
                async |i| {
                    assert_eq!(counter.load(Relaxed), i);
                    counter.store(i + 1, Relaxed);
                },
                None,
            )
            .await;
        assert_eq!(counter.load(Relaxed), 100);
    }

    #[tokio::test]
    async fn test_collect() {
        let items: Vec<u32> = futures::stream::iter(0..100).collect().await;
        let expected: Vec<u32> = (0..100).collect();
        assert_eq!(items, expected);
    }

    async fn foo(i: u32) -> u32 {
        static LOCK: Mutex<()> = Mutex::const_new(());
        let _guard = LOCK.lock();
        tokio::time::sleep(Duration::from_millis(rand::random_range(0..10))).await;
        i
    }

    #[tokio::test]
    async fn test_filter_map_concurrent() {
        let counter = AtomicU32::new(0);
        futures::stream::iter(0..100)
            .filter_map_concurrent(
                async |i| if i < 50 { Some(foo(i).await) } else { None },
                None,
            )
            .filter_map_concurrent(
                async |i| if i % 2 == 0 { Some(foo(i).await) } else { None },
                None,
            )
            .for_each_concurrent(
                async |i| {
                    assert_eq!(counter.load(Relaxed), i);
                    counter.store(i + 2, Relaxed);
                    // Try to provoke more deadlocks.
                    foo(i).await;
                },
                None,
            )
            .await;
        assert_eq!(counter.load(Relaxed), 50);
    }

    #[tokio::test]
    async fn test_filter_map_unordered() {
        let mut items: Vec<u32> = futures::stream::iter(0..100)
            .filter_map_unordered(
                async |i| if i < 50 { Some(foo(i).await) } else { None },
                None,
            )
            .filter_map_unordered(
                async |i| if i % 2 == 0 { Some(foo(i).await) } else { None },
                None,
            )
            .collect()
            .await;
        // Given the random sleeps, it's vanishingly unlikely that the order will match at first.
        let expected: Vec<u32> = (0..25).map(|i| 2 * i).collect();
        assert_ne!(items, expected);
        items.sort_unstable();
        assert_eq!(items, expected);
    }

    #[tokio::test]
    async fn test_buffered() {
        let counter = AtomicU32::new(0);
        futures::stream::iter((0..100).map(|i| foo(i)))
            .buffered(None)
            .for_each_concurrent(
                async |i| {
                    assert_eq!(counter.load(Relaxed), i);
                    counter.store(i + 1, Relaxed);
                    // Check for deadlocks.
                    foo(i).await;
                },
                None,
            )
            .await;
        assert_eq!(counter.load(Relaxed), 100);
    }

    #[tokio::test]
    async fn test_buffer_unordered() {
        let mut items: Vec<u32> = futures::stream::iter((0..100).map(|i| foo(i)))
            .buffer_unordered(None)
            .collect()
            .await;
        // Given the random sleeps, it's vanishingly unlikely that the order will match at first.
        let expected: Vec<u32> = (0..100).collect();
        assert_ne!(items, expected);
        items.sort_unstable();
        assert_eq!(items, expected);
    }
}
