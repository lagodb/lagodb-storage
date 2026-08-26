use super::*;

#[tokio::test]
async fn read_at_returns_bytes_without_advancing_cursor() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "read-at.txt").unwrap();
    let data = b"abcdefghijklmnop";
    let backend = MemoryObjectBackend::new();
    backend.insert(key, data.to_vec());

    let root = test_root("read-at-cache");
    let socket = test_root("read-at.sock");
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
        let file = client.open("bucket", "read-at.txt").unwrap();
        assert_eq!(file.position(), 0);

        // read_at at offset 4 returns 4 bytes
        let chunk = file.read_at(4, 4).unwrap();
        assert_eq!(&chunk, b"efgh");

        // cursor did NOT advance
        assert_eq!(file.position(), 0);

        // read_at at offset 0 returns the beginning
        let chunk2 = file.read_at(0, 3).unwrap();
        assert_eq!(&chunk2, b"abc");
        assert_eq!(file.position(), 0);

        // read_at past EOF returns empty
        let empty = file.read_at(100, 10).unwrap();
        assert!(empty.is_empty());
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn read_at_into_fills_buffer_without_advancing_cursor() {
    let key =
        ObjectLocation::new(TEST_STORE_ID, "bucket", "read-at-into.txt").unwrap();
    let data = b"0123456789";
    let backend = MemoryObjectBackend::new();
    backend.insert(key, data.to_vec());

    let root = test_root("read-at-into-cache");
    let socket = test_root("read-at-into.sock");
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
        let file = client.open("bucket", "read-at-into.txt").unwrap();
        let mut buf = [0u8; 5];
        let n = file.read_at_into(3, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"34567");
        assert_eq!(file.position(), 0);

        // empty buffer returns 0
        let n = file.read_at_into(0, &mut []).unwrap();
        assert_eq!(n, 0);
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn read_at_and_cursor_read_are_independent() {
    let key =
        ObjectLocation::new(TEST_STORE_ID, "bucket", "read-at-indep.txt").unwrap();
    let data = b"ABCDEFGHIJ";
    let backend = MemoryObjectBackend::new();
    backend.insert(key, data.to_vec());

    let root = test_root("read-at-indep-cache");
    let socket = test_root("read-at-indep.sock");
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
        let mut file = client.open("bucket", "read-at-indep.txt").unwrap();

        // Advance cursor to offset 5 via seek + cursor-based read
        file.seek(SeekFrom::Start(5));
        let cursor_data = file.read(3).unwrap();
        assert_eq!(&cursor_data, b"FGH");
        assert_eq!(file.position(), 8);

        // read_at at a different offset does not disturb the cursor
        let at_data = file.read_at(0, 3).unwrap();
        assert_eq!(&at_data, b"ABC");
        assert_eq!(file.position(), 8, "cursor must not move after read_at");

        // Continue cursor-based read from where it left off
        let more = file.read(2).unwrap();
        assert_eq!(&more, b"IJ");
        assert_eq!(file.position(), 10);
    })
    .await
    .unwrap();

    server_task.abort();
}
