use super::*;

#[tokio::test]
async fn large_partial_read_does_not_persist_metadata() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache.clone(),
    );
    let handles = service.test_context();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let read = read(&service, &handles, open.handle, 3, 5).await;

    assert_eq!(read.data, b"defgh");
    assert!(!read.eof);
    assert!(cache.index().get_meta(&key).await.unwrap().is_none());
    let partial = cache.live_large_fill_partial_path(&key).unwrap();
    assert!(tokio::fs::try_exists(partial).await.unwrap());

    close(&service, &handles, open.handle).await;
}

#[tokio::test]
async fn invalidate_large_fill_is_busy_while_handle_is_open() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache,
    );
    let handles = service.test_context();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let error = match service.execute(&handles, invalidate_cmd(LARGE_KEY)).await {
        Ok(_) => panic!("expected busy invalidate error"),
        Err(error) => error,
    };

    assert!(matches!(error, StorageError::Busy { .. }));
    close(&service, &handles, open.handle).await;
}

/// Regression guard for the session/reaper identity contract: if `invalidate_object_cache` races
/// ahead of a stale `ReapRequest` and admits a brand-new session for the same key, the stale
/// request must **not** delete the new partial or vacate the new registry entry.
///
/// Sequence modeled here:
/// 1. OPEN + first chunk read materialize a partial for session S1.
/// 2. The handle closes. S1's last `Arc` drops and enqueues a `ReapRequest` carrying S1's nonce.
///    Because the reaper and the foreground OPEN below both serialize on the same per-object
///    lock, we control whether the reaper runs first or after.
/// 3. A fresh OPEN admits session S2 (different nonce, same partial path). S2 reads and writes
///    the partial again.
/// 4. The assertion: while S2 is alive, the partial stays on disk and the registry still
///    resolves to S2. Either the reaper already processed the stale request (entry_matches ==
///    false, short-circuit) or it will process it after S2 drops (nonce does not match, still a
///    short-circuit). Neither outcome should touch S2's state.
#[tokio::test]
async fn stale_reap_request_does_not_clobber_newer_session() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache.clone(),
    );
    let handles = service.test_context();

    let first = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    // Partially fill so a partial file exists and a reap would have something to delete.
    assert_eq!(
        read(&service, &handles, first.handle, 0, 4).await.data,
        b"abcd"
    );
    let partial_path = cache.live_large_fill_partial_path(&key).unwrap();
    assert!(tokio::fs::try_exists(&partial_path).await.unwrap());

    close(&service, &handles, first.handle).await;

    // Before (or concurrently with) the reaper processing S1's request, admit S2 for the same
    // key. Whether the reaper runs before or after this OPEN, the stale nonce must prevent
    // cross-generation interference.
    let second = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    assert_eq!(
        read(&service, &handles, second.handle, 0, 4).await.data,
        b"abcd"
    );

    // Let any pending reaper work drain on the object lock.
    let new_partial = cache.live_large_fill_partial_path(&key).unwrap();
    assert_eq!(
        partial_path, new_partial,
        "partial path is deterministic per key"
    );
    assert!(
        tokio::fs::try_exists(&new_partial).await.unwrap(),
        "S2 partial must survive stale reap"
    );
    assert!(
        cache.has_live_large_fill(&key),
        "registry must still resolve to S2"
    );

    close(&service, &handles, second.handle).await;
}

#[tokio::test]
async fn last_large_fill_close_deletes_incomplete_partial() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache.clone(),
    );
    let handles = service.test_context();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let read = read(&service, &handles, open.handle, 0, 4).await;
    assert_eq!(read.data, b"abcd");
    let partial = cache.live_large_fill_partial_path(&key).unwrap();
    assert!(tokio::fs::try_exists(&partial).await.unwrap());

    close(&service, &handles, open.handle).await;
    // Large-fill cleanup runs on the cache manager's reaper task; close itself only releases the
    // per-handle Arc. Wait for the reaper to finalize — it will clear the registry entry and
    // unlink the partial.
    wait_until("reaper clears live large fill registry entry", || {
        !cache.has_live_large_fill(&key)
    })
    .await;
    wait_until_async("reaper unlinks partial payload", || async {
        !tokio::fs::try_exists(&partial).await.unwrap()
    })
    .await;
}

#[tokio::test]
async fn aborted_large_fill_rejects_new_open_until_last_close_finalizes() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend.clone()))
            .unwrap(),
        cache.clone(),
    );
    let handles = service.test_context();

    let old_open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    assert_eq!(
        read(&service, &handles, old_open.handle, 0, 4).await.data,
        b"abcd"
    );
    let old_partial = cache.live_large_fill_partial_path(&key).unwrap();
    let heads_after_old_open = backend.head_call_count();
    assert!(heads_after_old_open >= 1);

    cache.abort_live_large_fill_for_test(&key).await.unwrap();
    assert!(cache.has_live_large_fill(&key));

    let error = match service
        .execute(
            &handles,
            StorageCommand::Open(OpenCommand {
                bucket: BUCKET.to_string(),
                key: LARGE_KEY.to_string(),
                flags: OpenFlags::READ_ONLY,
            }),
        )
        .await
    {
        Ok(_) => panic!("aborted large fill should reject a new open"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StorageErrorKind::CacheFillAborted);
    // An aborted lifecycle must short-circuit inside the cache: no HEAD, no backend I/O.
    assert_eq!(
        backend.head_call_count(),
        heads_after_old_open,
        "rejecting an aborted lifecycle must not issue a backend HEAD",
    );

    close(&service, &handles, old_open.handle).await;
    // Partial unlink and registry vacancy are driven by the reaper task.
    wait_until_async("reaper unlinks aborted partial", || async {
        !tokio::fs::try_exists(&old_partial).await.unwrap()
    })
    .await;
    wait_until("reaper clears aborted session entry", || {
        !cache.has_live_large_fill(&key)
    })
    .await;

    let new_open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let new_partial = cache.live_large_fill_partial_path(&key).unwrap();
    assert_eq!(old_partial, new_partial);
    assert_eq!(
        read(&service, &handles, new_open.handle, 0, 4).await.data,
        b"abcd"
    );
    assert!(tokio::fs::try_exists(&new_partial).await.unwrap());
    close(&service, &handles, new_open.handle).await;
}

#[tokio::test]
async fn stale_partial_from_failed_unlink_is_truncated_on_next_fill() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache.clone(),
    );
    let handles = service.test_context();

    // Plant a stale partial twice the object size with a recognizable byte pattern. If the new
    // session failed to truncate on first write, chunk 0 would leave trailing stale bytes that
    // surface in later reads and break commit equality.
    let partial = cache.partial_path(&key).unwrap();
    write_cache_file(partial.clone(), &[0xCCu8; 20]).await;
    assert!(!cache.has_live_large_fill(&key));

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let new_partial = cache.live_large_fill_partial_path(&key).unwrap();
    assert_eq!(partial, new_partial);

    // First read triggers the first chunk write: the partial must be opened with O_TRUNC so no
    // stale 0xCC bytes remain beyond the written range.
    let first_read = read(&service, &handles, open.handle, 0, 4).await;
    assert_eq!(first_read.data, b"abcd");
    let after_first_write = tokio::fs::read(&partial).await.unwrap();
    assert_eq!(
        after_first_write.len(),
        4,
        "stale bytes past the first chunk must be truncated"
    );
    assert_eq!(after_first_write, b"abcd");

    // Finish the fill to confirm promotion still yields the backend's bytes, not a mix with the
    // stale payload.
    let tail = read(&service, &handles, open.handle, 4, 6).await;
    assert_eq!(tail.data, b"efghij");
    assert!(tail.eof);
    let complete = tokio::fs::read(cache.complete_path(&key).unwrap())
        .await
        .unwrap();
    assert_eq!(complete, b"abcdefghij");

    close(&service, &handles, open.handle).await;
}
