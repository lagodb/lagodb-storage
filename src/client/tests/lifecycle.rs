use super::*;

#[tokio::test]
async fn dropping_storage_file_closes_server_handle_best_effort() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "drop-close.txt").unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghijklmnop".to_vec());

    let root = test_root("drop-close-cache");
    let socket = test_root("drop-close.sock");
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
    let service = Arc::new(StorageService::with_registry(registry, cache.clone()));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let active_fds = Arc::new(AtomicUsize::new(0));
    let active_fds_for_client = Arc::clone(&active_fds);
    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect_managed_with_fd_policy(
            &client_socket,
            TEST_VOLUME_ID,
            Box::new(CountingFdPolicy {
                active: Arc::clone(&active_fds_for_client),
                limit: None,
            }),
        )
        .unwrap();
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 1);

        let mut file = client.open("bucket", "drop-close.txt").unwrap();
        let data = file.read(16).unwrap();
        assert_eq!(data, b"abcdefghijklmnop");
        file.close().unwrap();
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 1);

        let mut file = client.open("bucket", "drop-close.txt").unwrap();
        assert!(file.is_direct_io());
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 2);
        file.close().unwrap();
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 1);
        let error = file.read_at(0, 1).unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::ClosedHandle);
        drop(file);
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 1);

        drop(client);
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 0);
    })
    .await
    .unwrap();
    assert_eq!(active_fds.load(Ordering::SeqCst), 0);

    let mut policy = CacheCleanupPolicy::new(16);
    policy.cleanup_start_ratio = 0.0;
    policy.cleanup_target_ratio = 0.0;
    let report = cache.run_cleanup_for_test(Some(policy)).await.unwrap();

    assert_eq!(report.evicted_objects, 1);
    assert_eq!(cache.logical_cache_usage().await.unwrap().resident_bytes, 0);
    assert!(
        !tokio::fs::try_exists(cache.complete_path(&key).unwrap())
            .await
            .unwrap()
    );

    let limited_fds = Arc::new(AtomicUsize::new(0));
    let limited_fds_for_client = Arc::clone(&limited_fds);
    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect_managed_with_fd_policy(
            &client_socket,
            TEST_VOLUME_ID,
            Box::new(CountingFdPolicy {
                active: Arc::clone(&limited_fds_for_client),
                limit: Some(1),
            }),
        )
        .unwrap();
        let mut warming_file = client.open("bucket", "drop-close.txt").unwrap();
        assert!(!warming_file.is_direct_io());
        assert_eq!(warming_file.read(16).unwrap(), b"abcdefghijklmnop");
        warming_file.close().unwrap();

        let error = match client.open("bucket", "drop-close.txt") {
            Ok(_) => panic!("direct open unexpectedly exceeded the FD budget"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StorageErrorKind::ResourceExhausted);
        assert!(!client.is_usable());
        assert_eq!(limited_fds_for_client.load(Ordering::SeqCst), 0);
    })
    .await
    .unwrap();
    assert_eq!(limited_fds.load(Ordering::SeqCst), 0);

    server_task.abort();
}

#[tokio::test]
async fn panic_unwind_poisons_connection_and_allows_reconnect() {
    let key =
        ObjectLocation::new(TEST_STORE_ID, "bucket", "unwind-close.txt").unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"small object".to_vec());

    let root = test_root("unwind-close-cache");
    let socket = test_root("unwind-close.sock");
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(64, 64),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, Arc::new(backend))
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind_with_config(
        &socket,
        service,
        StorageServerConfig::default().with_max_open_handles_per_connection(1),
    )
    .await
    .unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _file = client.open("bucket", "unwind-close.txt").unwrap();
            panic!("injected error-report unwind");
        }));
        assert!(unwind.is_err());
        assert!(!client.is_usable());

        let replacement =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let mut reopened = replacement.open("bucket", "unwind-close.txt").unwrap();
        assert_eq!(reopened.read(12).unwrap(), b"small object");
        reopened.close().unwrap();
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn cached_bytes_are_shared_across_credentials_for_one_physical_identity() {
    let root = test_root("client-store-cache");
    let socket = test_root("client-store.sock");
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let config_a = StoreConfig::S3Compatible(S3CompatibleStoreConfig {
        endpoint: "http://127.0.0.1:9".to_string(),
        region: Some("us-east-1".to_string()),
        access_key_id: Some(SecretString::new("access-a")),
        secret_access_key: Some(SecretString::new("secret-a")),
        token: None,
        allow_http: true,
        virtual_hosted_style_request: false,
        skip_signature: false,
        encryption: None,
    });
    let config_b = StoreConfig::S3Compatible(S3CompatibleStoreConfig {
        access_key_id: Some(SecretString::new("access-b")),
        secret_access_key: Some(SecretString::new("secret-b")),
        ..match config_a.clone() {
            StoreConfig::S3Compatible(config) => config,
            _ => unreachable!(),
        }
    });
    let identity = BackendDataIdentity::from_config(&config_a);
    assert_eq!(identity, BackendDataIdentity::from_config(&config_b));
    let location = ObjectLocation::new(identity, "bucket", "file").unwrap();
    let cached_bytes = b"shared-cache-body".to_vec();
    cache
        .index()
        .admit_small_if_absent(
            CachedObjectMeta::small(
                location,
                ObjectInfo {
                    size: cached_bytes.len() as u64,
                    etag: Some("cached".to_string()),
                },
                cached_bytes.len() as u64,
            ),
            cached_bytes.clone(),
            1,
        )
        .await
        .unwrap();
    let service =
        Arc::new(StorageService::with_registry(StoreRegistry::new(), cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let first =
            StorageClient::connect_configured(&client_socket, Arc::new(config_a))
                .unwrap();
        let second =
            StorageClient::connect_configured(&client_socket, Arc::new(config_b))
                .unwrap();
        let mut first_file = first.open("bucket", "file").unwrap();
        let mut second_file = second.open("bucket", "file").unwrap();
        assert_eq!(first_file.read(64).unwrap(), cached_bytes);
        assert_eq!(second_file.read(64).unwrap(), cached_bytes);
    })
    .await
    .unwrap();

    server_task.abort();
}
