use super::*;

#[tokio::test]
async fn client_stage_write_upload_roundtrip_uploads_to_backend() {
    let root = test_root("staging-cache");
    let socket = test_root("staging.sock");
    let staging_uploader = Arc::new(StagingUploader::new(root.clone()));
    let staging_root = root.clone();

    let backend = Arc::new(MemoryObjectBackend::new());
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let config = crate::config::StorageServiceConfig::default();
    let service = Arc::new(StorageService::with_staging_uploader(
        registry,
        cache,
        staging_uploader,
        config,
    ));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    let upload_info = tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(staging_root);
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let mut staging_file = StagingFile::create(
            &resolver,
            client.backend_identity(),
            "bucket",
            "uploaded.txt",
        )
        .unwrap();
        staging_file.write(b"hello ").unwrap();
        staging_file.write(b"upload").unwrap();
        staging_file.sync().unwrap();
        drop(staging_file);
        client.upload("bucket", "uploaded.txt").unwrap()
    })
    .await
    .unwrap();

    assert_eq!(upload_info.size, b"hello upload".len() as u64);
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "uploaded.txt").unwrap();
    let data = backend
        .get_range(key.path(), 0..upload_info.size)
        .await
        .unwrap();
    assert_eq!(&data[..], b"hello upload");

    server_task.abort();
}

#[tokio::test]
async fn caller_unlinks_staging_file_to_discard_without_upload() {
    // The database (caller) owns the staging directory: a transaction abort, or any other
    // reason to discard a staged file before upload, is implemented by unlinking the path
    // it created via `StagingFile::create`. The server has no abort verb.
    let root = test_root("staging-discard-cache");
    let socket = test_root("staging-discard.sock");
    let staging_uploader = Arc::new(StagingUploader::new(root.clone()));
    let staging_root = root.clone();

    let backend = Arc::new(MemoryObjectBackend::new());
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let config = crate::config::StorageServiceConfig::default();
    let service = Arc::new(StorageService::with_staging_uploader(
        registry,
        cache,
        staging_uploader,
        config,
    ));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    let staging_path = tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(staging_root);
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let mut staging_file = StagingFile::create(
            &resolver,
            client.backend_identity(),
            "bucket",
            "discarded.txt",
        )
        .unwrap();
        staging_file.write(b"doomed").unwrap();
        let path = staging_file.path().to_path_buf();
        drop(staging_file);
        // Caller-side cleanup: ordinary filesystem unlink. Idempotent because the database is
        // the only writer of this path.
        std::fs::remove_file(&path).unwrap();
        path
    })
    .await
    .unwrap();

    assert!(
        !tokio::fs::try_exists(&staging_path).await.unwrap(),
        "caller-issued unlink must remove the staging file",
    );
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "discarded.txt").unwrap();
    assert!(
        backend.get_range(key.path(), 0..1).await.is_err(),
        "discarded key must not have been uploaded to the backend"
    );

    server_task.abort();
}

#[tokio::test]
async fn client_upload_can_be_issued_from_a_different_connection() {
    let root = test_root("staging-xconn-cache");
    let socket = test_root("staging-xconn.sock");
    let staging_uploader = Arc::new(StagingUploader::new(root.clone()));
    let staging_root = root.clone();

    let backend = Arc::new(MemoryObjectBackend::new());
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let config = crate::config::StorageServiceConfig::default();
    let service = Arc::new(StorageService::with_staging_uploader(
        registry,
        cache,
        staging_uploader,
        config,
    ));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(staging_root);
        let mut staging_file = StagingFile::create(
            &resolver,
            &test_identity(),
            "bucket",
            "cross-conn.txt",
        )
        .unwrap();
        staging_file.write(b"cross-connection upload").unwrap();
        drop(staging_file);
    })
    .await
    .unwrap();

    let socket_for_uploader = socket.clone();
    let upload_info = tokio::task::spawn_blocking(move || {
        let uploader =
            StorageClient::connect_managed(&socket_for_uploader, TEST_VOLUME_ID)
                .unwrap();
        uploader.upload("bucket", "cross-conn.txt").unwrap()
    })
    .await
    .unwrap();

    assert_eq!(upload_info.size, b"cross-connection upload".len() as u64);
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "cross-conn.txt").unwrap();
    let readback = backend
        .get_range(key.path(), 0..upload_info.size)
        .await
        .unwrap();
    assert_eq!(&readback[..], b"cross-connection upload");

    server_task.abort();
}

#[tokio::test]
async fn stage_twice_without_finalize_returns_busy() {
    let root = test_root("staging-busy-cache");
    tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(root);
        let _first = StagingFile::create(
            &resolver,
            &test_identity(),
            "bucket",
            "duplicate.txt",
        )
        .unwrap();
        let error = StagingFile::create(
            &resolver,
            &test_identity(),
            "bucket",
            "duplicate.txt",
        )
        .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Busy);
    })
    .await
    .unwrap();
}

#[test]
fn staging_file_holds_external_fd_lease_until_drop() {
    let root = test_root("staging-fd-accounting");
    let resolver = StagingPathResolver::new(root);
    let active_fds = Arc::new(AtomicUsize::new(0));
    let policy = CountingFdPolicy {
        active: Arc::clone(&active_fds),
        limit: None,
    };

    let staging = StagingFile::create_with_fd_policy(
        &resolver,
        &test_identity(),
        "bucket",
        "accounted.txt",
        &policy,
    )
    .unwrap();
    assert_eq!(active_fds.load(Ordering::SeqCst), 1);

    drop(staging);
    assert_eq!(active_fds.load(Ordering::SeqCst), 0);

    let error = StagingFile::create_with_fd_policy(
        &resolver,
        &test_identity(),
        "bucket",
        "accounted.txt",
        &policy,
    )
    .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Busy);
    assert_eq!(active_fds.load(Ordering::SeqCst), 0);
}
