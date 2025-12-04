use futures::channel::mpsc::{Receiver, channel};
use futures::{SinkExt, StreamExt, join};

pub struct AsyncPipeline<Fut: Future, T> {
    future: Fut,
    outputs: Receiver<T>,
}

impl<Fut: Future, T> AsyncPipeline<Fut, T> {
    pub fn then<F, U>(mut self, mut f: impl AsyncFnMut(T) -> U) -> AsyncPipeline<impl Future, U> {
        let (mut sender, receiver) = channel(0);
        AsyncPipeline {
            future: async {
                join! {
                    self.future,
                    async move {
                        while let Some(input) = self.outputs.next().await {
                            let output = f(input).await;
                            sender.send(output).await.unwrap();
                        }
                    }
                }
            },
            outputs: receiver,
        }
    }
}
