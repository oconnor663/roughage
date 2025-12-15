# `roughage` [![crates.io](https://img.shields.io/crates/v/roughage.svg)](https://crates.io/crates/roughage) [![docs.rs](https://docs.rs/roughage/badge.svg)](https://docs.rs/roughage)

This crate provides a single type, [`AsyncPipeline`], which is an alternative to [`buffered`]
streams, [`FuturesOrdered`], and [`FuturesUnordered`].

All of those are prone to deadlocks if any of their buffered/concurrent futures touches an
async lock of any kind, _even indirectly_. (For example, note that [`tokio::sync::mpsc`]
channels [use a `Semaphore` internally][internally].) The problem is that they don't
consistently poll their buffered futures, so a future holding a lock could stop making forward
progress through no fault of its own. `AsyncPipeline` fixes this whole class of deadlocks by
consistently polling all its in-flight futures until they complete. In other words,
`AsyncPipeline` will never "snooze" a future.

[`AsyncPipeline`]: https://docs.rs/roughage/latest/roughage/struct.AsyncPipeline.html
[`buffered`]: https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.buffered
[`FuturesOrdered`]: https://docs.rs/futures/latest/futures/stream/struct.FuturesOrdered.html
[`FuturesUnordered`]: https://docs.rs/futures/latest/futures/stream/struct.FuturesUnordered.html
[`tokio::sync::mpsc`]: https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html
[internally]: https://github.com/tokio-rs/tokio/blob/0ec0a8546105b9f250f868b77e42c82809703aab/tokio/src/sync/mpsc/bounded.rs#L162

Here's how easy it is to provoke a deadlock with `buffered` streams:

```rust
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};

static LOCK: Mutex<()> = Mutex::const_new(());

// An innocent example function that touches an async lock. Note
// that the deadlocks below can happen even if this function is
// buried three crates deep in some dependency you never see.
async fn foo() {
    let _guard = LOCK.lock().await;
    sleep(Duration::from_millis(1)).await;
}

futures::stream::iter([foo(), foo()])
    .buffered(2)
    .for_each(|_| async {
        foo().await; // Deadlock!
    })
    .await;
```

Here's the same deadlock with `FuturesUnordered`:

```rust
let mut unordered = futures::stream::FuturesUnordered::new();
unordered.push(foo());
unordered.push(foo());
while let Some(_) = unordered.next().await {
    foo().await; // Deadlock!
}
```

An `AsyncPipeline` does not have this problem, because once it's started a future internally,
it never stops polling it:

```rust
use roughage::AsyncPipeline;

AsyncPipeline::from_iter(0..100)
    .map_concurrent(|_| foo(), 10)
    .map_unordered(|_| foo(), 10)
    .for_each_concurrent(|_| foo(), 10)
    .await;
// Deadlock free!
```

See [`AsyncPipeline`] for more examples.

"Roughage" (_ruff_-_edge_) is an older term for dietary fiber. It keeps our pipes running
smoothly.
