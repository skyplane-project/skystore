use std::pin::Pin;

use flo_stream::MessagePublisher;
use s3s::dto::StreamingBlob;

use s3s::stream::ByteStream;
use s3s::stream::RemainingLength;

use tokio_stream::Stream;
use tokio_stream::StreamExt;

/// A wrapper around a stream that transforms its items into `Result<T, Box<dyn std::error::Error + Send + Sync>>`.
struct WrapToResultStream<S, T>
where
    S: Stream<Item = T> + Unpin,
{
    inner: S,
    size_hint: RemainingLength,
}

impl<S, T> WrapToResultStream<S, T>
where
    S: Stream<Item = T> + Unpin,
{
    fn new(inner: S, size_hint: RemainingLength) -> Self {
        Self { inner, size_hint }
    }
}

impl<S, T> tokio_stream::Stream for WrapToResultStream<S, T>
where
    S: Stream<Item = T> + Unpin,
{
    type Item = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(cx);
        match result {
            std::task::Poll::Ready(Some(item)) => std::task::Poll::Ready(Some(Ok(item))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<S, T> s3s::stream::ByteStream for WrapToResultStream<S, T>
where
    S: Stream<Item = T> + Unpin,
{
    fn remaining_length(&self) -> s3s::stream::RemainingLength {
        s3s::stream::RemainingLength::new_exact(self.size_hint.exact().unwrap())
    }
}

pub fn split_streaming_blob(
    incoming: StreamingBlob,
    num_splits: usize,
) -> (Vec<StreamingBlob>, Vec<i64>) {
    // Effectively an unbounded buffer.
    let mut publisher = flo_stream::Publisher::new(usize::MAX);

    // size_hint is required so the S3 client can set content length header properly.
    // that's why we just need it once in the beginning and keep it static.
    let hint = incoming.remaining_length();

    let mut result: Vec<StreamingBlob> = Vec::new();
    let mut size: Vec<i64> = Vec::new();
    for _ in 0..num_splits {
        let sub = publisher.subscribe();
        let stream =
            WrapToResultStream::new(sub, RemainingLength::new_exact(hint.exact().unwrap()));
        let sub_blob = StreamingBlob::new(stream);
        let stream_size = sub_blob.remaining_length().exact().unwrap();
        result.push(sub_blob);
        size.push(stream_size as i64);
    }

    // TODO: make return a JoinHandle if caller needs it.
    tokio::spawn(async move {
        // doing this because Error is not clonable.
        let infallable_blob = incoming.map(|res| res.unwrap());
        let stream_publisher = flo_stream::StreamPublisher::new(&mut publisher, infallable_blob);
        stream_publisher.await;
        drop(publisher);
    });

    (result, size)
}

#[cfg(test)]
mod test_flo {
    use super::*;

    use s3s::Body;

    #[tokio::test]
    async fn test_flo() {
        async fn test_sub_blob(sub_blob: StreamingBlob, expected_body: Vec<u8>) {
            let result_bytes = sub_blob
                .map(|chunk| chunk.unwrap())
                .collect::<Vec<_>>()
                .await;
            let body = result_bytes.concat();
            assert!(body == expected_body);
        }

        let blob: StreamingBlob = StreamingBlob::from(Body::from("hello world".to_string()));

        let (mut out, _) = split_streaming_blob(blob, 2);
        assert!(out.len() == 2);

        let sub_blob1 = out.pop().unwrap();
        let sub_blob2 = out.pop().unwrap();

        test_sub_blob(sub_blob1, "hello world".to_string().into_bytes()).await;
        test_sub_blob(sub_blob2, "hello world".to_string().into_bytes()).await;
    }
}
