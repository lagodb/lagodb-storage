use super::*;

#[tokio::test]
async fn disconnect_aborts_in_flight_request_tasks() {
    let (server_stream, mut client_stream) = UnixStream::pair().unwrap();
    let (started_send, started_recv) = oneshot::channel();
    let (dropped_send, dropped_recv) = oneshot::channel();
    let backend = HangingBackend {
        started: Arc::new(Mutex::new(Some(started_send))),
        dropped: Arc::new(Mutex::new(Some(dropped_send))),
    };
    let cache = Arc::new(
        CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, Arc::new(backend))
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let context = service.test_context();
    let server_task = tokio::spawn(process_connection_with_shutdown(
        server_stream,
        context,
        test_server_config(1),
        test_shutdown(Duration::from_millis(50)),
    ));

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 1,
            payload: WireRequestPayload::Open {
                bucket: "bucket".to_string(),
                key: "file".to_string(),
                flags: OpenFlags::READ_ONLY,
            },
        },
    )
    .await;
    let handle = match read_response(&mut client_stream).await {
        WireResponsePayload::Open { handle, .. } => handle,
        other => panic!("unexpected open response: {other:?}"),
    };

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 2,
            payload: WireRequestPayload::Read {
                handle,
                offset: 0,
                len: 4,
            },
        },
    )
    .await;

    timeout(Duration::from_secs(1), started_recv)
        .await
        .unwrap()
        .unwrap();
    drop(client_stream);
    timeout(Duration::from_secs(1), dropped_recv)
        .await
        .unwrap()
        .unwrap();
    let result = timeout(Duration::from_secs(1), server_task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn write_half_shutdown_still_receives_in_flight_response() {
    let (server_stream, mut client_stream) = UnixStream::pair().unwrap();
    let (started_send, started_recv) = oneshot::channel();
    let (release_send, release_recv) = oneshot::channel();
    let backend = DelayedHeadBackend {
        started: Mutex::new(Some(started_send)),
        release: Mutex::new(Some(release_recv)),
    };
    let cache = Arc::new(
        CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, Arc::new(backend))
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let context = service.test_context();
    let server_task = tokio::spawn(process_connection_with_shutdown(
        server_stream,
        context,
        test_server_config(1),
        test_shutdown(Duration::from_secs(1)),
    ));

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 1,
            payload: WireRequestPayload::Open {
                bucket: "bucket".to_string(),
                key: "file".to_string(),
                flags: OpenFlags::READ_ONLY,
            },
        },
    )
    .await;
    timeout(Duration::from_secs(1), started_recv)
        .await
        .unwrap()
        .unwrap();
    client_stream.shutdown().await.unwrap();
    release_send.send(()).unwrap();

    match timeout(Duration::from_secs(1), read_response(&mut client_stream))
        .await
        .unwrap()
    {
        WireResponsePayload::Open {
            size, direct_io, ..
        } => {
            assert_eq!(size, 10);
            assert!(!direct_io);
        }
        other => panic!("unexpected open response: {other:?}"),
    }

    let result = timeout(Duration::from_secs(1), server_task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn close_waits_for_prior_read_on_same_handle() {
    let (server_stream, mut client_stream) = UnixStream::pair().unwrap();
    let (started_send, started_recv) = oneshot::channel();
    let (release_send, release_recv) = oneshot::channel();
    let backend = DelayedRangeBackend {
        started: Mutex::new(Some(started_send)),
        release: Mutex::new(Some(release_recv)),
    };
    let cache = Arc::new(
        CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, Arc::new(backend))
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let context = service.test_context();
    let server_task = tokio::spawn(process_connection_with_shutdown(
        server_stream,
        context,
        test_server_config(2),
        test_shutdown(Duration::from_secs(1)),
    ));

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 1,
            payload: WireRequestPayload::Open {
                bucket: "bucket".to_string(),
                key: "file".to_string(),
                flags: OpenFlags::READ_ONLY,
            },
        },
    )
    .await;
    let handle = match read_response(&mut client_stream).await {
        WireResponsePayload::Open { handle, .. } => handle,
        other => panic!("unexpected open response: {other:?}"),
    };

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 2,
            payload: WireRequestPayload::Read {
                handle,
                offset: 0,
                len: 4,
            },
        },
    )
    .await;
    timeout(Duration::from_secs(1), started_recv)
        .await
        .unwrap()
        .unwrap();

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 3,
            payload: WireRequestPayload::Close { handle },
        },
    )
    .await;
    assert!(
        timeout(Duration::from_millis(30), read_frame(&mut client_stream))
            .await
            .is_err()
    );

    release_send.send(()).unwrap();
    match timeout(Duration::from_secs(1), read_response(&mut client_stream))
        .await
        .unwrap()
    {
        WireResponsePayload::Read { data, eof } => {
            assert_eq!(data, b"abcd");
            assert!(!eof);
        }
        other => panic!("unexpected first response: {other:?}"),
    }
    assert!(matches!(
        timeout(Duration::from_secs(1), read_response(&mut client_stream))
            .await
            .unwrap(),
        WireResponsePayload::Close
    ));

    drop(client_stream);
    let result = timeout(Duration::from_secs(1), server_task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn read_admission_precedes_response_budget_wait() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "file").unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = Arc::new(
        CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, Arc::new(backend))
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let context = service.test_context();
    let backend = context.attached().unwrap().backend();
    let state = context
        .handles
        .open(
            key,
            backend,
            ObjectInfo {
                size: 10,
                etag: None,
            },
        )
        .unwrap();
    let mut tasks = RequestTasks::new();
    let request_limiter = Arc::new(Semaphore::new(2));
    let response_byte_limiter = ResponseByteLimiter::new(0);
    let (response_tx, mut response_rx) = mpsc::channel::<QueuedResponse>(2);

    tasks
        .spawn_request(
            WireRequest {
                request_id: 1,
                payload: WireRequestPayload::Read {
                    handle: state.handle,
                    offset: 0,
                    len: 4,
                },
            },
            context.clone(),
            request_limiter.clone(),
            response_byte_limiter.clone(),
            4,
            response_tx.clone(),
        )
        .await
        .unwrap();
    tasks
        .spawn_request(
            WireRequest {
                request_id: 2,
                payload: WireRequestPayload::Close {
                    handle: state.handle,
                },
            },
            context,
            request_limiter,
            response_byte_limiter,
            4,
            response_tx,
        )
        .await
        .unwrap();

    assert!(
        timeout(Duration::from_millis(50), response_rx.recv())
            .await
            .is_err()
    );
    tasks.abort_all().await;
}

#[tokio::test]
async fn inbound_close_uses_one_total_drain_budget() {
    let (response_tx, _response_rx) = mpsc::channel::<QueuedResponse>(1);
    let mut request_tasks = RequestTasks::new();
    request_tasks.spawn_background(async {
        sleep(Duration::from_millis(60)).await;
    });
    let writer_task =
        tokio::spawn(async { future::pending::<StorageResult<()>>().await });
    let mut writer = ResponseWriter::from_parts_for_test(response_tx, writer_task);

    let result = timeout(
        Duration::from_millis(120),
        test_shutdown(Duration::from_millis(80)).drain_on_inbound_closed(
            &mut request_tasks,
            &mut writer,
            "test-client",
        ),
    )
    .await
    .expect("shutdown should not get a second drain timeout");

    assert!(result.is_ok());
}
