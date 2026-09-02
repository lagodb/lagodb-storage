# lagodb-storage Design Rationale & Architecture

This document details the architectural principles, consistency invariants, concurrency model, and subsystem designs behind `lagodb-storage`. It focuses on **why** the system is built this way and how its core mechanisms operate together.

---

## 1. Positioning & Design Philosophy

`lagodb-storage` is a user-space caching and transaction staging engine co-located on the same host as the database process (such as PostgreSQL Lakebase). It sits between the query execution engine and remote cloud object stores (Amazon S3, Google Cloud Storage, Azure Blob, and MinIO).

Rather than acting as a general-purpose FUSE filesystem or generic HTTP proxy, `lagodb-storage` is specialized for analytical database lakehouse workloads:

* **Zero-IPC Direct I/O via FD Passing**: When reading objects fully cached on local disk, the daemon transfers the open file descriptor to the database process via `SCM_RIGHTS`. The database reads directly from the OS page cache via `pread`, eliminating IPC serialization, memory copies, and daemon bottlenecks.
* **Zero-Probe Cache Hits**: Cache hits never issue remote `HEAD` probes, background polling, or distributed coherency handshakes to check freshness — reading cached data requires zero network round-trips.
* **Path-Handoff Transaction Staging**: In-flight database transactions write new objects directly to local filesystem staging paths using standard OS syscalls. The daemon maintains zero in-memory state during data generation and asynchronously streams the closed file to object storage upon explicit `Upload`.
* **Disposable Derived-Data Safety**: Cached bytes are strictly treated as derived state from remote object stores. System crashes or process restarts leave no corrupt metadata, and startup reconciliation recovers state instantly.
* **Host-Local UDS Transport**: Eliminates TCP stack overhead with binary-framed Unix Domain Sockets featuring bounded response memory queues, request multiplexing, and deterministic backpressure.

---

## 2. Core Design Invariants & Architectural Contracts

`lagodb-storage` relies on a set of explicit, well-defined invariants across its cache, staging, and connection subsystems. These contracts eliminate distributed state reconciliation, prevent data corruption, and ensure deterministic behavior.

### 2.1 Cache Subsystem Invariants

1. **Immutable Metadata Facts**: Once `size` and `etag` enter the cache index (`redb`) or an in-progress large-fill session, they become immutable facts for that cache lifecycle. The daemon does not reconcile, overwrite, or mutate those values from later backend observations.
2. **Single Physical Residency (No Generations)**: One credential-free physical tuple `(BackendDataIdentity, bucket, key)` maps to at most one cached residency at a time. The system does not maintain concurrent generational versions for the same key.
3. **External Invalidation Boundary**: Cache freshness is governed externally. The only boundary that retires an active cache residency is an explicit `invalidate_object_cache(bucket, key)` command from an attached client (or capacity-driven eviction, which is not a freshness event). A cache hit never issues a remote `HEAD` probe to check for same-key changes.
4. **The Cache is Derived Data**: Every cached byte is derived from the remote object backend. A crash between writing a cache file and updating metadata leaves an unindexed orphan file on disk, not a corruption. Startup recovery and runtime janitor passes safely reclaim orphan files.
5. **Frozen Handle Residency**: The `Open` command determines and binds the object residency (`SmallKv`, `CompleteFile`, or `LargeFill`) for the lifetime of that `FileHandle`. Subsequent `Read` operations on that handle execute directly against the bound residency and never re-probe the index (achieving zero-KV-lookup reads).

### 2.2 Staging Subsystem Invariants

1. **Append-Only Local Writes**: Staging files are opened with `O_APPEND`, ensuring bytes always append strictly at EOF.
2. **Single Active Writer**: Only one active `StagingFile` may exist per `(BackendDataIdentity, bucket, key)` at a time. `StagingFile::create` opens files with `O_CREAT | O_EXCL`, ensuring that racing writers on the same physical key fail atomically with `StorageErrorKind::Busy`.
3. **Caller-Owned Lifecycle & Cleanup**: The caller owns the staging directory's lifecycle. Cleanup—after a successful upload, on transaction rollback before upload, or during database crash recovery—is performed directly by the database through standard filesystem syscalls. The daemon exposes no stage-create, abort, or startup wipe verbs.
4. **No Readers Before Upload/Publication**: Staged bytes are invisible to query readers until `Upload` succeeds and the caller commits its transaction. Staging paths remain completely uncoupled from the read cache.

### 2.3 Connection & Context Invariants

1. **Mandatory Single Attach Handshake**: Every client socket must attach exactly one backend context (`AttachManaged` or `AttachConfigured`) before request multiplexing begins. After attachment, all subsequent object operations carry only `(bucket, key)`, decoupling catalog or database session identities from the wire protocol.
2. **Credential-Agnostic Physical Deduplication**: `BackendDataIdentity` captures physical provider endpoint and bucket parameters while strictly excluding credentials (e.g. access keys, session tokens). Multiple connections accessing the same physical bucket share cache residency without duplicating disk footprints or re-authorizing secondary credentials.
3. **Synchronous Client / Asynchronous Server Boundary**: The client library exposes a synchronous, blocking API designed for embedding into C/FFI extensions, PostgreSQL backends, and foreign runtimes without requiring a client-side async event loop. All asynchronous concurrency, Tokio scheduling, and chunk multiplexing remain strictly encapsulated inside the daemon.

---

## 3. Layered System Architecture

```
 +-------------------------------------------------------------------------+
 |             Database Process (LagoDB / PostgreSQL Engine)               |
 |                                                                         |
 |  +-----------------------+                    +----------------------+  |
 |  | StorageClient         |                    | Local Direct I/O     |  |
 |  | (Sync / Zero Event    |                    | - Direct pread (FD)  |  |
 |  |  Loop for C / FFI)    |                    | - Staging appends    |  |
 |  +-----------+-----------+                    +----------^-----------+  |
 +--------------|-------------------------------------------|--------------+
                | Unix Domain Socket                        | Direct FS
                | (Control Frames & SCM_RIGHTS FD Passing)  | / Page Cache
 +--------------v-------------------------------------------|--------------+
 | lagodb-storage Daemon                                    |              |
 |                                                          |              |
 |  +----------------------------------------------------+  |              |
 |  | Transport & Framing  (UDS Frame Codec / FD Channel)|  |              |
 |  +----------------------------------------------------+  |              |
 |  | Connection Manager   (Pipelines / Backpressure)    |  |              |
 |  +----------------------------------------------------+  |              |
 |  | Service Dispatcher   (Attach / Open / Read / Upload|  |              |
 |  +--------------------+-------------------------------+  |              |
 |                       |                                  |              |
 |         +-------------+-------------+                    |              |
 |         |                           |                    |              |
 |  +------v-----------------+  +------v-----------------+  |              |
 |  | Two-Tier Cache         |  | Staging Subsystem      |  |              |
 |  | - redb KV (<=4KB & Idx)|  | - Staging Path Resolver|  |              |
 |  | - Chunk Store (32MB)   |  | - Async Put Uploader   |  |              |
 |  | - LRU / Watermark Clean|  +------------+-----------+  |              |
 |  +------------+-----------+               |              |              |
 |               |                           |              |              |
 |               +-------------+-------------+              |              |
 |                             |                            |              |
 |                      +------v-----------------+          |              |
 |                      | Multi-Cloud Backends   |          |              |
 |                      | (S3 / GCS / Azure/MinIO|          |              |
 |                      +--------------+---------+          |              |
 +-------------------------------------|--------------------v--------------+
                                       | Async GET / PUT     Local Cache &
                                       | (object_store)      Staging Disk
                                +------v-----------------+
                                |  Cloud Object Storage  |
                                +------------------------+
```

### Component Responsibilities

* **Transport**: Serializes/deserializes length-prefixed binary wire frames and manages ancillary UNIX domain socket control messages (`SCM_RIGHTS`).
* **Connection**: Manages socket lifecycles, per-connection request semaphore budgets, bounded response backlog queues, and graceful connection draining.
* **Service**: Validates attached backend contexts, resolves handle reservations, routes storage commands, and enforces service-level read limits.
* **Two-Tier Cache**: Manages the embedded metadata/KV store (`redb`), on-disk chunked payload files, single-flight fill sessions, and capacity eviction scheduling.
* **Staging Subsystem**: Derives deterministic staging file paths for in-flight transaction writes and executes streaming backend multipart/file uploads.
* **Backend Layer**: Wraps cloud object store SDKs (`object_store` for AWS S3, Google Cloud Storage, Azure Blob, and in-memory test mocks) behind unified asynchronous traits.

---

## 4. Read Path & Two-Tier Cache Engine

### 4.1 Two-Tier Storage Architecture
To prevent filesystem inode exhaustion and block slack wasted on tiny objects, `lagodb-storage` divides cached data into two distinct tiers:

1. **Small-Object KV Store ($\le 4\text{ KiB}$)**: Objects smaller than or equal to the configurable threshold are stored directly as embedded values inside the `redb` database alongside metadata rows. Cache hits return in-memory slices without disk file creation.
2. **Large-Object Chunk Store ($> 4\text{ KiB}$)**: Objects exceeding the threshold are partitioned into fixed-size chunks (default $32\text{ MiB}$) stored under the filesystem directory tree (`partial/` during fill, promoted to `complete/` when fully cached).

### 4.2 Frozen Handle Residency
When a client sends an `Open` request:
1. The server determines the object residency (`SmallKv`, `CompleteFile`, or `LargeFill`).
2. The server binds the residency to a connection-local `FileHandle` and registers it in the `HandleTable`.
3. **Residency is frozen for the handle's lifetime.** Subsequent `Read` operations on that handle execute directly against the bound residency without re-probing the index.

### 4.3 Zero-IPC Direct I/O via FD Passing
When `Open` resolves to a `CompleteFile` residency:
1. The server opens a read-only OS file descriptor on the cached file.
2. The server serializes the `Open` reply frame and attaches the file descriptor as an `SCM_RIGHTS` ancillary control message.
3. The client receives the `OwnedFd` and constructs a `ReadPath::Direct`.
4. Subsequent calls to `read_at` or `read_into` on the client execute `pread` directly against the local descriptor:
   * **Data never flows through the UDS socket.**
   * **The daemon user-space process is completely bypassed.**
   * Reads achieve bare-metal OS page cache and NVMe speed.

```
  [ Direct Read Flow — Fully Cached Large Object (Zero-IPC) ]

    Database Client                      lagodb-storage                   Local Disk
          |                                     |                              |
      (1) |--- OPEN(bucket, key) -------------->|                              |
          |                                     |--- Lookup Cache (Complete) ->|
          |                                     |<-- Open File Handle ---------|
          |<-- Open Response + SCM_RIGHTS(FD) --|                              |
          |    (ReadPath::Direct)               |                              |
          |                                                                    |
      (2) |=== Direct pread(offset, len) via OS Page Cache ==================>|
          |<== Zero-Copy Data (Bypasses lagodb-storage completely) ============|
          |                                                                    |
      (3) |--- CLOSE(handle) ------------------>| (releases server lease)      |
```

### 4.4 Large Object Streaming & Single-Flight Chunk Fill
When an object is not fully cached on local disk:
1. **Establishment Election**: The first request for a cold object becomes the *leader*, issues a `HEAD` request to the backend, and initializes a `LargeFillSession`. Concurrent requests for the same key join the active session as *followers*.
2. **Chunk-Level Streaming**: The leader streams $32\text{ MiB}$ chunks from the remote backend into a local partial file (`cache/partial/...`).
3. **Concurrent Follower Reads**: Followers read chunks from the partial file as soon as the leader writes and flushes each chunk.
4. **Atomic Promotion**: Once all chunks have arrived, the partial file is atomically renamed to the complete path (`cache/complete/...`), and metadata is committed to `redb`.

```
  [ Cache Miss Read Flow — Large Object Chunk Streaming ]

    Database Client                      lagodb-storage               Remote Object Store
          |                                     |                              |
      (1) |--- OPEN(bucket, key) -------------->|                              |
          |                                     |--- HEAD & Init Fill Session->|
          |<-- Open Response (LargeFill) -------|                              |
          |                                     |                              |
      (2) |--- READ(offset, len) -------------->|                              |
          |                                     |--- GET Range Chunk (32MB) -->|
          |                                     |<-- Chunk Data ---------------|
          |                                     |--- Write to Partial File --->[Local Disk]
          |<-- Streamed Chunk Payload ----------|                              |
          |                                     |                              |
          |                                     | (When all chunks arrive)     |
          |                                     | Promote partial -> complete  |
```

---

## 5. Write Path (Path-Handoff Staging)

### 5.1 Local Append-Only Staging Contract
For write workflows (e.g. database query outputs, bulk loads, compaction):
1. **Zero-IPC Creation**: The database derives the local staging path using `StagingPathResolver` and creates the file with `O_CREAT | O_EXCL | O_APPEND | O_CLOEXEC` via `StagingFile::create`.
2. **Kernel-Direct Appends**: The database writes data directly to the local filesystem using standard file syscalls. The daemon does not participate in or buffer write streams.
3. **No Intermediate Server State**: Long-running transactions hold no server-side memory or open socket state during data preparation.

### 5.2 Server-Side Async Upload
When the transaction completes writing and closes the local staging file:
1. The database sends an `Upload(bucket, key)` request over an attached connection.
2. The server reads the closed staging file from disk and streams it to the remote object backend using optimized multipart or single-part `put_from_file`.
3. The server returns the final object metadata (`size`, `etag`) to the database.

```
  [ Staging Write Flow — Transactional Path-Handoff ]

    Database Client                     Local Filesystem                lagodb-storage
          |                                     |                              |
      (1) |--- StagingFile::create(...) ------->| (O_CREAT | O_EXCL)           |
          |--- Direct append-only writes ------>| (Zero Server Overhead)       |
          |--- Close staging file ------------->|                              |
          |                                                                    |
      (2) |--- UPLOAD(bucket, key) ------------------------------------------->|
          |                                     |<-- Read Staged File ---------|
          |                                     |--- Stream PUT to Backend --->[Cloud Store]
          |<-- Upload Outcome (ETag, Size) ------------------------------------|
          |                                                                    |
      (3) |--- Unlink local staging file ------>|                              |
          |--- (Optional) Invalidate Cache ---->| (If overwriting existing key)|
```

### 5.3 Staging Lifecycle Ownership
* **Caller Owns the Staging Tree**: The database owns the staging directory. Cleanup—after successful upload, on transaction rollback, or during database crash recovery—is performed directly by the database via ordinary filesystem syscalls.
* **No Daemon Rollback RPCs**: The daemon exposes no stage-create, abort, or rollback verbs. If a transaction fails before upload, the database simply deletes the local staging file.

---

## 6. Concurrency Model & Memory Guardrails

```
  [ Connection Pipeline Architecture ]

  Client Socket
       |
       v
  +----+---------------------------------------------------------------+
  | Read Task (Tokio)                                                  |
  | - Decodes binary wire frames                                       |
  | - Acquires request semaphore permit (max_in_flight_requests)       |
  | - Spawns per-request dispatch tasks                                |
  +----+---------------------------------------------------------------+
       |
       v (concurrent dispatch)
  +----+---------------------------------------------------------------+
  | Request Tasks                                                      |
  | - Reserve response budget (max_pending_response_bytes)             |
  | - Execute command (Cache / Backend / Staging)                      |
  | - Enqueue response item to bounded mpsc channel                    |
  +----+---------------------------------------------------------------+
       |
       v
  +----+---------------------------------------------------------------+
  | Single-Writer Task                                                 |
  | - Pulls from response channel                                      |
  | - Serializes wire frames and ancillary FDs                         |
  | - Flushes to Unix Domain Socket                                    |
  +--------------------------------------------------------------------+
```

### 6.1 Concurrency & Backpressure Guardrails

* **Request Semaphore**: Each connection limits concurrent requests with an asynchronous semaphore (`max_in_flight_requests`, default 256), preventing client flooding.
* **Dual-Bounded Response Queue**: The response channel enforces both an item count limit (`max_pending_responses`, default 64) and a payload byte budget (`max_pending_response_bytes`, default 32 MiB). Fast storage readers cannot exhaust server memory when serving slow consumers.
* **Pre-Allocated Read Budgets**: Mediated read commands reserve their buffer byte budget before performing disk I/O, preventing sudden allocation spikes.
* **Response Write Timeout**: Unresponsive or blocked socket clients are disconnected after a configurable timeout (`response_write_timeout`, default 30s), reclaiming buffers.
* **Orderly Draining on Half-Close**: When a client half-closes its socket (`EOF`), the connection enters an orderly draining state machine: new requests are rejected, in-flight tasks are given a grace period (`connection_drain_timeout`, default 2s), and the writer closes cleanly.

---

## 7. Capacity Management & Eviction Engine

### 7.1 CleanupScheduler Architecture
All eviction triggers and janitor passes feed into a single actor (`CleanupScheduler`), preventing concurrent or overlapping disk traversals:

* **Event-Driven Write Nudges**: Every cache admission performs an allocation-free check against `max_cache_bytes`. If resident bytes exceed `cleanup_start_percent` (default 90%), a nudge is sent to the scheduler to evict least-recently-used items down to `cleanup_target_percent` (default 80%).
* **Periodic Janitor (Safety Net)**: An optional periodic interval scans for orphaned partial files, reconciles disk state, and runs eviction passes.
* **Hot-Reload Support**: Cache limits, watermarks, and batch sizes can be reconfigured online via `StorageRuntime::apply` without restarting the daemon.

### 7.2 Active Handle Protection
The eviction engine strictly respects active read leases. If an object is selected for LRU eviction while one or more `FileHandle` instances are open:
* The object's file on disk and metadata entry are protected from eviction.
* Reading queries are never interrupted by background capacity management.

---

## 8. Crash-Recovery & Derived-Data Boundary

The cache splits durable index metadata from non-durable physical chunk files:

1. **Chunk Isolation**: Unfinished chunk writes land in `cache/partial/` and process memory.
2. **Atomic Promotion**: Only when all chunks of an object are verified on disk is the file renamed to `cache/complete/` and metadata written to `redb`.
3. **Crash Recovery**: If the system crashes mid-fill, the partial file is left behind as an unindexed orphan. At server startup, the recovery pass scans `cache/partial/` and removes unclaimed files.
4. **Derived Data Contract**: If a complete file is deleted externally while metadata remains, the server returns an I/O error rather than attempting complex self-repair. The caller invalidates the key or re-opens to trigger fresh retrieval.

---

## 9. Observability & Error Handling

* **Structured Tracing**: All internal components emit structured `tracing` spans and events with correlation fields (`handle`, `key`, `size`, `direct_io`, `residency`).
* **Typed Error Model**: All errors map to structured `StorageError` categories (`NotFound`, `Busy`, `Backend`, `Protocol`, `CorruptIndex`, `Io`), giving embedding applications precise failure visibility without parsing error strings.
