use super::*;

fn complete_mock_attach(stream: &mut std::os::unix::net::UnixStream) {
    let frame = read_frame_blocking(stream).unwrap().unwrap();
    let request = decode_request(&frame).unwrap();
    assert!(matches!(
        request.payload,
        WireRequestPayload::AttachManaged { .. }
    ));
    let response = encode_response(&WireResponse {
        request_id: request.request_id,
        payload: WireResponsePayload::Attach {
            backend_identity: test_identity().cache_key().to_owned(),
        },
    })
    .unwrap();
    write_frame_blocking(stream, &response).unwrap();
}

#[test]
fn client_poisons_connection_after_response_id_mismatch() {
    let socket = test_root("poison-mismatched-response.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        complete_mock_attach(&mut stream);
        let _request = read_frame_blocking(&mut stream).unwrap().unwrap();
        let response = encode_response(&WireResponse {
            request_id: 99,
            payload: WireResponsePayload::Head {
                size: 1,
                etag: None,
            },
        })
        .unwrap();
        write_frame_blocking(&mut stream, &response).unwrap();
    });

    let active_fds = Arc::new(AtomicUsize::new(0));
    let client = StorageClient::connect_managed_with_fd_policy(
        &socket,
        TEST_VOLUME_ID,
        Box::new(CountingFdPolicy {
            active: Arc::clone(&active_fds),
            limit: None,
        }),
    )
    .unwrap();
    assert_eq!(active_fds.load(Ordering::SeqCst), 1);
    let error = client.head("bucket", "object").unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Protocol);
    assert!(!client.is_usable());
    assert_eq!(active_fds.load(Ordering::SeqCst), 0);

    let second = client.head("bucket", "object").unwrap_err();
    assert_eq!(second.kind(), StorageErrorKind::Protocol);
    server.join().unwrap();
}

#[test]
fn client_keeps_connection_after_remote_operation_error() {
    let socket = test_root("remote-error-keeps-connection.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        complete_mock_attach(&mut stream);
        let _first = read_frame_blocking(&mut stream).unwrap().unwrap();
        let not_found = encode_response(&WireResponse::error(
            2,
            StorageError::not_found("missing"),
        ))
        .unwrap();
        write_frame_blocking(&mut stream, &not_found).unwrap();

        let _second = read_frame_blocking(&mut stream).unwrap().unwrap();
        let found = encode_response(&WireResponse {
            request_id: 3,
            payload: WireResponsePayload::Head {
                size: 7,
                etag: None,
            },
        })
        .unwrap();
        write_frame_blocking(&mut stream, &found).unwrap();
    });

    let client = StorageClient::connect_managed(&socket, TEST_VOLUME_ID).unwrap();
    let error = client.head("bucket", "missing").unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::NotFound);
    assert!(client.is_usable());
    assert_eq!(client.head("bucket", "present").unwrap().size, 7);
    server.join().unwrap();
}

#[tokio::test]
async fn client_delete_removes_object_from_backend_and_is_idempotent() {
    let root = test_root("delete-cache");
    let socket = test_root("delete.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "doomed.txt").unwrap();
    backend.insert(key.clone(), b"bye".to_vec());
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
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let socket_for_client = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client =
            StorageClient::connect_managed(&socket_for_client, TEST_VOLUME_ID)
                .unwrap();
        client.delete("bucket", "doomed.txt").unwrap();
        // Second delete is idempotent regardless of backend's missing-key behavior.
        client.delete("bucket", "doomed.txt").unwrap();
    })
    .await
    .unwrap();

    assert!(
        backend.get_range(key.path(), 0..1).await.is_err(),
        "deleted key must not survive in the backend"
    );

    server_task.abort();
}

#[tokio::test]
async fn client_delete_prefix_removes_all_matching_objects_and_rejects_empty_prefix()
{
    let root = test_root("delete-prefix-cache");
    let socket = test_root("delete-prefix.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    for key in ["scope/a", "scope/b", "scope/nested/c", "other/d"] {
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", key).unwrap(),
            b"x".to_vec(),
        );
    }
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
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let socket_for_client = socket.clone();
    let deleted = tokio::task::spawn_blocking(move || {
        let client =
            StorageClient::connect_managed(&socket_for_client, TEST_VOLUME_ID)
                .unwrap();

        let empty_prefix_error = client.delete_prefix("bucket", "").unwrap_err();
        assert_eq!(empty_prefix_error.kind(), StorageErrorKind::InvalidPath);

        client.delete_prefix("bucket", "scope/").unwrap()
    })
    .await
    .unwrap();

    assert_eq!(deleted, 3);
    let surviving = ObjectLocation::new(TEST_STORE_ID, "bucket", "other/d").unwrap();
    assert!(
        backend.get_range(surviving.path(), 0..1).await.is_ok(),
        "delete_prefix must not touch keys outside the prefix"
    );
    for gone in ["scope/a", "scope/b", "scope/nested/c"] {
        let key = ObjectLocation::new(TEST_STORE_ID, "bucket", gone).unwrap();
        assert!(
            backend.get_range(key.path(), 0..1).await.is_err(),
            "key {gone} should have been deleted by delete_prefix",
        );
    }

    server_task.abort();
}

#[tokio::test]
async fn client_bulk_delete_and_explicit_cursor_close_are_end_to_end() {
    let root = test_root("bulk-delete-cache");
    let socket = test_root("bulk-delete.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    for key in ["scope/a", "scope/b", "scope/c"] {
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", key).unwrap(),
            b"x".to_vec(),
        );
    }
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
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let socket_for_client = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client =
            StorageClient::connect_managed(&socket_for_client, TEST_VOLUME_ID)
                .unwrap();
        let first_page = client.list_page("bucket", Some("scope/"), None, 1).unwrap();
        let cursor = first_page.next_cursor.expect("more objects must remain");
        client.close_list_cursor(cursor.clone()).unwrap();
        let closed = match client.list_page("bucket", Some("scope/"), Some(cursor), 1)
        {
            Ok(_) => panic!("closed cursor must be rejected"),
            Err(error) => error,
        };
        assert_eq!(closed.kind(), StorageErrorKind::ExpiredCursor);

        let deleted = client
            .delete_objects(
                "bucket",
                vec!["scope/a".to_owned(), "scope/b".to_owned()],
            )
            .unwrap();
        assert_eq!(deleted, 2);
    })
    .await
    .unwrap();

    for gone in ["scope/a", "scope/b"] {
        let key = ObjectLocation::new(TEST_STORE_ID, "bucket", gone).unwrap();
        assert!(backend.get_range(key.path(), 0..1).await.is_err());
    }
    let survivor = ObjectLocation::new(TEST_STORE_ID, "bucket", "scope/c").unwrap();
    assert!(backend.get_range(survivor.path(), 0..1).await.is_ok());

    server_task.abort();
}

#[tokio::test]
async fn client_list_iterates_pages_and_returns_every_object() {
    let root = test_root("list-cache");
    let socket = test_root("list.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    let mut expected: Vec<String> = Vec::with_capacity(50);
    for i in 0..50 {
        let key = format!("scope/{i:03}");
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", key.clone()).unwrap(),
            b"x".to_vec(),
        );
        expected.push(key);
    }
    expected.sort();
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
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let socket_for_client = socket.clone();
    let listed = tokio::task::spawn_blocking(move || {
        let client =
            StorageClient::connect_managed(&socket_for_client, TEST_VOLUME_ID)
                .unwrap();
        let mut keys: Vec<String> = client
            .list("bucket", Some("scope/"))
            .map(|item| item.unwrap().key)
            .collect();
        keys.sort();
        keys
    })
    .await
    .unwrap();

    assert_eq!(listed, expected);

    server_task.abort();
}

#[tokio::test]
async fn client_list_session_owns_connection_and_drives_pages() {
    let root = test_root("list-page-cache");
    let socket = test_root("list-page.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    for i in 0..5 {
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", format!("k/{i}")).unwrap(),
            b"x".to_vec(),
        );
    }
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
        .with_shared_backend(TEST_STORE_ID, backend)
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let socket_for_client = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client =
            StorageClient::connect_managed(&socket_for_client, TEST_VOLUME_ID)
                .unwrap();
        let mut listing = client.list_session("bucket", Some("k/"), 2);
        drop(client);

        let page1 = listing.next_page().unwrap().expect("first page must exist");
        assert_eq!(page1.len(), 2);

        let page2 = listing
            .next_page()
            .unwrap()
            .expect("second page must exist");
        assert_eq!(page2.len(), 2);

        let page3 = listing.next_page().unwrap().expect("final page must exist");
        assert_eq!(page3.len(), 1);
        assert!(listing.next_page().unwrap().is_none());
    })
    .await
    .unwrap();

    server_task.abort();
}
