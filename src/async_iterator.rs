use futures::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait AsyncIterator {
    type Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        _ = cx;
        Poll::Ready(())
    }

    fn buffered(self, n: usize) -> Buffered<Self>
    where
        Self: Sized,
    {
        Buffered {
            iter: self,
            items: VecDeque::with_capacity(n),
        }
    }

    fn filter_map_ordered<F, Fut, L>(self, f: F, limit: L) -> Buffered<Self>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future,
        L: Into<Option<usize>>,
    {
        FlatMap {
            executor: crate::Executor<Fut>,
            iter: self,
            items: VecDeque::with_capacity(n),
        }
    }

    fn for_each<F, Fut>(self, f: F) -> ForEach<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = ()>,
    {
        ForEach {
            iter: self,
            f,
            fut: None,
        }
    }
}

impl<S: Stream> AsyncIterator for S {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Stream::poll_next(self, cx)
    }
}

pin_project_lite::pin_project! {
    pub struct Buffered<I>
    where
        I: AsyncIterator,
    {
        #[pin]
        iter: I,
        items: VecDeque<I::Item>,
    }
}

impl<I> AsyncIterator for Buffered<I>
where
    I: AsyncIterator,
{
    type Item = I::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if let Some(item) = this.items.pop_front() {
            return Poll::Ready(Some(item));
        }
        this.iter.poll_next(cx)
    }

    fn poll_progress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut this = self.project();
        while this.items.len() < this.items.capacity() {
            match this.iter.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.items.push_back(item);
                }
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(())
    }
}

pin_project_lite::pin_project! {
    pub struct ForEach<I, F, Fut>
    where
        I: AsyncIterator,
        F: FnMut(I::Item) -> Fut,
        Fut: Future<Output = ()>,
    {
        #[pin]
        iter: I,
        f: F,
        #[pin]
        fut: Option<Fut>,
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
            if let Some(future) = this.fut.as_mut().as_pin_mut() {
                match future.poll(cx) {
                    Poll::Ready(()) => {
                        this.fut.set(None);
                    }
                    Poll::Pending => {
                        _ = this.iter.poll_progress(cx);
                        return Poll::Pending;
                    }
                }
            }
            debug_assert!(this.fut.is_none());
            match this.iter.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let fut = (this.f)(item);
                    this.fut.set(Some(fut));
                    // Continue the loop.
                }
                Poll::Ready(None) => {
                    return Poll::Ready(());
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}
