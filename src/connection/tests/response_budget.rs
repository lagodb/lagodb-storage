use super::*;

#[tokio::test]
async fn response_byte_limiter_waits_until_permit_is_released() {
    let limiter = ResponseByteLimiter::new(4);
    let first = limiter.acquire(4).await.unwrap();

    assert!(
        timeout(Duration::from_millis(20), limiter.acquire(1))
            .await
            .is_err()
    );

    drop(first);
    timeout(Duration::from_secs(1), limiter.acquire(4))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn queued_read_response_holds_byte_budget_until_dropped() {
    let limiter = ResponseByteLimiter::new(4);
    let response_bytes = limiter.acquire(4).await.unwrap();
    let queued = QueuedResponse::new(
        StorageHandlerResponse {
            request_id: 1,
            payload: StorageHandlerPayload::Read {
                body: ReadBody::Bytes(bytes::Bytes::from_static(&[1, 2, 3, 4])),
                eof: false,
            },
            direct_file: None,
        },
        response_bytes,
    );

    assert!(
        timeout(Duration::from_millis(20), limiter.acquire(1))
            .await
            .is_err()
    );

    drop(queued);
    timeout(Duration::from_secs(1), limiter.acquire(4))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn queued_non_read_response_releases_reserved_read_budget() {
    let limiter = ResponseByteLimiter::new(4);
    let response_bytes = limiter.acquire(4).await.unwrap();
    let _queued = QueuedResponse::new(
        StorageHandlerResponse {
            request_id: 1,
            payload: StorageHandlerPayload::Wire(WireResponsePayload::Close),
            direct_file: None,
        },
        response_bytes,
    );

    timeout(Duration::from_secs(1), limiter.acquire(4))
        .await
        .unwrap()
        .unwrap();
}
