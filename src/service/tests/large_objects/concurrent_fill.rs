use super::*;

#[tokio::test]
async fn abort_large_fill_wakes_waiting_chunk_followers() {
    let key = default_location(LARGE_KEY);
    let inner_backend = MemoryObjectBackend::new();
    inner_backend.insert(key.clone(), b"abcdefgh".to_vec());
    let backend = Arc::new(BlockingRangeBackend::new(inner_backend));
    let cache = memory_cache();
    let service = Arc::new(StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, backend.clone())
            .unwrap(),
        cache.clone(),
    ));
    let handles = service.test_context();

    let leader_open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let follower_open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let leader_handle = leader_open.handle;
    let follower_handle = follower_open.handle;

    let leader_service = service.clone();
    let leader_handles = handles.clone();
    let leader_read = tokio::spawn(async move {
        leader_service
            .execute(
                &leader_handles,
                StorageCommand::Read(ReadCommand {
                    handle: leader_handle,
                    offset: 0,
                    len: 4,
                }),
            )
            .await
    });
    backend.wait_until_first_range_get_starts().await;

    let follower_service = service.clone();
    let follower_handles = handles.clone();
    let follower_read = tokio::spawn(async move {
        follower_service
            .execute(
                &follower_handles,
                StorageCommand::Read(ReadCommand {
                    handle: follower_handle,
                    offset: 0,
                    len: 4,
                }),
            )
            .await
    });
    tokio::task::yield_now().await;

    cache.abort_live_large_fill_for_test(&key).await.unwrap();

    let follower = tokio::time::timeout(Duration::from_secs(1), follower_read)
        .await
        .expect("follower read should wake after abort")
        .expect("follower read task should not panic");
    assert!(matches!(
        follower,
        Err(StorageError::CacheFillAborted { .. })
    ));

    backend.release_first_range_get();
    let leader = tokio::time::timeout(Duration::from_secs(1), leader_read)
        .await
        .expect("leader read should finish after backend unblocks")
        .expect("leader read task should not panic");
    assert!(matches!(leader, Err(StorageError::CacheFillAborted { .. })));

    service
        .execute(
            &handles,
            StorageCommand::Close(CloseCommand {
                handle: leader_handle,
            }),
        )
        .await
        .unwrap();
    service
        .execute(
            &handles,
            StorageCommand::Close(CloseCommand {
                handle: follower_handle,
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn large_open_joins_live_fill_without_second_head() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend.clone()))
            .unwrap(),
        cache,
    );
    let handles = service.test_context();

    let first = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let second = open_file(&service, &handles, BUCKET, LARGE_KEY).await;

    let first_state = handles.handles.get(first.handle).unwrap();
    let second_state = handles.handles.get(second.handle).unwrap();
    assert_eq!(backend.head_call_count(), 1);
    let first_session = first_state
        .residency
        .large_fill_session()
        .expect("large fill");
    let second_session = second_state
        .residency
        .large_fill_session()
        .expect("large fill");
    assert!(Arc::ptr_eq(&first_session, &second_session));

    close(&service, &handles, first.handle).await;
    close(&service, &handles, second.handle).await;
}
