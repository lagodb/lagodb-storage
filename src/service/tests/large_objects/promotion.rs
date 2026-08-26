use super::*;

#[tokio::test]
async fn live_large_fill_is_removed_after_last_open_close() {
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
    let second = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    assert!(cache.has_live_large_fill(&key));

    close(&service, &handles, first.handle).await;
    assert!(cache.has_live_large_fill(&key));

    close(&service, &handles, second.handle).await;
    assert!(!cache.has_live_large_fill(&key));
}

#[tokio::test]
async fn large_full_read_commits_complete_metadata() {
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
    let partial = cache.live_large_fill_partial_path(&key).unwrap();
    let read = read(&service, &handles, open.handle, 0, 10).await;

    assert_eq!(read.data, b"abcdefghij");
    assert!(read.eof);
    let meta = cache.index().get_meta(&key).await.unwrap().unwrap();
    assert_eq!(meta.cache_state(), CacheState::CompleteFile);
    assert!(
        tokio::fs::try_exists(cache.complete_path(&key).unwrap())
            .await
            .unwrap()
    );
    assert!(!tokio::fs::try_exists(partial).await.unwrap());

    close(&service, &handles, open.handle).await;
}

#[tokio::test]
async fn large_full_read_replaces_unclaimed_complete_payload() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let complete_path = cache.complete_path(&key).unwrap();
    write_cache_file(complete_path.clone(), b"stale").await;
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache.clone(),
    );
    let handles = service.test_context();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let read = read(&service, &handles, open.handle, 0, 10).await;

    assert_eq!(read.data, b"abcdefghij");
    assert!(read.eof);
    assert_eq!(tokio::fs::read(complete_path).await.unwrap(), b"abcdefghij");
    assert_eq!(
        cache
            .index()
            .get_meta(&key)
            .await
            .unwrap()
            .unwrap()
            .cache_state(),
        CacheState::CompleteFile
    );

    close(&service, &handles, open.handle).await;
}

#[tokio::test]
async fn concurrent_large_reads_commit_complete_metadata_once() {
    use crate::cache::{CacheManager, InMemoryCacheIndex};
    use crate::config::{StorageRuntime, StorageRuntimeConfig};

    let key = default_location(LARGE_KEY);
    let inner_backend = MemoryObjectBackend::new();
    inner_backend.insert(key.clone(), b"abcdefgh".to_vec());
    let backend = Arc::new(BlockingRangeBackend::new(inner_backend));
    let index = CountingCompleteIndex::new(InMemoryCacheIndex::new());
    let runtime = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
    let cache = Arc::new(
        CacheManager::new(super::super::fixtures::test_cache_dir(), index, runtime)
            .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let service = Arc::new(StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, backend.clone())
            .unwrap(),
        cache.clone(),
    ));
    let handles = service.test_context();

    let first = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let second = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let partial = cache.live_large_fill_partial_path(&key).unwrap();

    let first_service = service.clone();
    let first_handles = handles.clone();
    let first_read = tokio::spawn(async move {
        read(&first_service, &first_handles, first.handle, 0, 8).await
    });
    backend.wait_until_first_range_get_starts().await;

    let second_service = service.clone();
    let second_handles = handles.clone();
    let second_read = tokio::spawn(async move {
        read(&second_service, &second_handles, second.handle, 0, 8).await
    });
    tokio::task::yield_now().await;
    backend.release_first_range_get();

    let first = first_read.await.unwrap();
    let second = second_read.await.unwrap();

    assert_eq!(first.data, b"abcdefgh");
    assert_eq!(second.data, b"abcdefgh");
    assert!(first.eof);
    assert!(second.eof);
    assert_eq!(cache.index().complete_puts(), 1);
    assert_eq!(backend.range_gets(), 2);
    assert_eq!(
        cache
            .index()
            .get_meta(&key)
            .await
            .unwrap()
            .unwrap()
            .cache_state(),
        CacheState::CompleteFile
    );
    assert!(
        tokio::fs::try_exists(cache.complete_path(&key).unwrap())
            .await
            .unwrap()
    );
    assert!(!tokio::fs::try_exists(partial).await.unwrap());
}
