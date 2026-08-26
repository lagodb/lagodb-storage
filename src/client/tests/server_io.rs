use super::*;

#[tokio::test]
async fn client_reads_through_unix_socket_server() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "hello.txt").unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"hello from storage".to_vec());

    let root = test_root("cache");
    let socket = test_root("storage.sock");
    let cache = Arc::new(
        CacheManager::new(
            root.clone(),
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
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let mut file = client.open("bucket", "hello.txt").unwrap();
        file.seek(SeekFrom::Start(6));
        let mut data = [0_u8; 4];
        let n = file.read_into(&mut data).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&data, b"from");
        file.close().unwrap();
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn client_head_and_exists_do_not_admit_cache() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "head.txt").unwrap();
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(key, b"hello metadata".to_vec());

    let root = test_root("head-cache");
    let socket = test_root("head.sock");
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache.clone()));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let info = client.head("bucket", "head.txt").unwrap();
        assert_eq!(info.size, b"hello metadata".len() as u64);
        assert!(client.exists("bucket", "head.txt").unwrap());
        assert!(!client.exists("bucket", "missing.txt").unwrap());
    })
    .await
    .unwrap();

    assert_eq!(backend.head_call_count(), 3);
    assert_eq!(cache.logical_cache_usage().await.unwrap().resident_bytes, 0);

    server_task.abort();
}

#[tokio::test]
async fn client_head_returns_from_cache_after_open() {
    let data = b"cached content";
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "cached.txt").unwrap();
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(key, data.to_vec());

    let root = test_root("head-cached");
    let socket = test_root("head-cached.sock");
    // small_object_limit must be >= data length so the object is admitted as SmallKv
    // (large-fill objects don't persist metadata to the index until fill completes).
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(data.len() as u64, 4096),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache.clone()));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();

        // Open admits the object into the cache (triggers one backend head during establishment).
        let mut file = client.open("bucket", "cached.txt").unwrap();
        file.close().unwrap();
        let heads_after_open = backend.head_call_count();

        // Subsequent head calls should be served from the cache index — no new backend heads.
        let info = client.head("bucket", "cached.txt").unwrap();
        assert_eq!(info.size, data.len() as u64);

        let info2 = client.head("bucket", "cached.txt").unwrap();
        assert_eq!(info2.size, data.len() as u64);

        assert_eq!(
            backend.head_call_count(),
            heads_after_open,
            "head should be served from cache, not backend"
        );
    })
    .await
    .unwrap();

    server_task.abort();
}
