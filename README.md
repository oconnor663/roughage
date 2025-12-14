# `AsyncPipeline` [![crates.io](https://img.shields.io/crates/v/async_pipeline.svg)](https://crates.io/crates/async_pipeline) [![docs.rs](https://docs.rs/async_pipeline/badge.svg)](https://docs.rs/async_pipeline)

This crate is a replacement for [`buffered`] streams, [`FuturesOrdered`], and
[`FuturesUnordered`], which are broken if any future in the buffer touches any async lock
([`Mutex`], [`RwLock`], [`Semaphore`], etc.), even indirectly through abstractions like
[`tokio::sync::mpsc`] channels (which [use a `Semaphore` internally][internally]).

[`buffered`]: https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.buffered
[`FuturesOrdered`]: https://docs.rs/futures/latest/futures/stream/struct.FuturesOrdered.html
[`FuturesUnordered`]: https://docs.rs/futures/latest/futures/stream/struct.FuturesUnordered.html
[`Mutex`]: https://docs.rs/tokio/latest/tokio/sync/struct.Mutex.html
[`RwLock`]: https://docs.rs/tokio/latest/tokio/sync/struct.RwLock.html
[`Semaphore`]: https://docs.rs/tokio/latest/tokio/sync/struct.Semaphore.html
[`tokio::sync::mpsc`]: https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html
[internally]: https://github.com/tokio-rs/tokio/blob/0ec0a8546105b9f250f868b77e42c82809703aab/tokio/src/sync/mpsc/bounded.rs#L162

Here's how easy it is to provoke a deadlock with `buffered` streams:

```rust
static LOCK: Mutex<()> = Mutex::const_new(());

// An example function that touches a lock. No tricks here!
async fn foo() {
    let _guard = LOCK.lock().await;
    sleep(Duration::from_millis(1)).await;
}

futures::stream::iter([foo(), foo()])
    .buffered(2)
    .for_each(|_| async {
        foo().await; // deadlock!
    })
    .await;
```

Here's the same deadlock with `FuturesUnordered`:

```rust
let mut unordered = FuturesUnordered::new();
unordered.push(foo());
unordered.push(foo());
while let Some(_) = unordered.next().await {
    foo().await; // deadlock!
}
```

An `AsyncPipeline` does not have this problem, because once it's started a future internally,
it never stops polling it:

```rust
AsyncPipeline::new(futures::stream::iter(0..100))
    .map_concurrent(|_| foo(), 10)
    .map_unordered(|_| foo(), 10)
    .for_each_concurrent(|_| foo(), 10)
    .await;
// Deadlock free!
```
