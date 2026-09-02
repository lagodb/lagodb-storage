# lagodb-storage

[![CI](https://github.com/lagodb/lagodb-storage/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/lagodb/lagodb-storage/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A high-performance, user-space object storage caching and transaction staging daemon tailored for [LagoDB](https://github.com/lagodb/lagodb) and modern cloud-native analytical database engines.

`lagodb-storage` co-locates with database processes to bridge low-latency transactional analytics with scalable cloud object stores (Amazon S3, Google Cloud Storage, Azure Blob, and MinIO). Through **zero-IPC descriptor passing (`SCM_RIGHTS`)**, an intelligent **two-tier cache engine**, and a **path-handoff staging model**, it delivers direct disk throughput for reads and writes while managing object lifecycles asynchronously.

Originally engineered as the core object storage caching and transaction staging subsystem for [LagoDB](https://github.com/lagodb/lagodb), `lagodb-storage` is maintained as an independent, standalone daemon and storage service.

---

## Why lagodb-storage?

Cloud object storage offers virtually infinite capacity at low cost, but high time-to-first-byte (TTFB), request rate limits, and the lack of POSIX random-access semantics make direct querying prohibitive for high-performance database engines.

Traditional approaches (such as FUSE filesystems, kernel mounts, or generic HTTP caching proxies) introduce significant kernel context switches, coarse-grained buffer invalidations, and heavy distributed cache-coherence protocols. `lagodb-storage` adopts a specialized, database-first architecture:

* **Zero-IPC Direct I/O via FD Passing**: When reading objects already cached on local disk, the daemon passes the open file descriptor directly to the database process. Reads execute via kernel `pread` directly against the OS page cache, completely bypassing the daemon and IPC socket buffers.
* **Zero-Probe Cache Hits**: Cache hits never issue remote `HEAD` probes or background polling to check freshness — reading cached data requires zero network round-trips.
* **Path-Handoff Transaction Staging**: In-flight database transactions write new objects directly to local filesystem staging paths using standard file syscalls. The daemon stays entirely out of the write loop until the file is closed and ready for background upload.
* **Zero-Corruption Derived Data**: The local cache is strictly derived state. Crashes leave no corrupt metadata, incomplete chunks are cleaned automatically, and startup reconciliation recovers state instantly.
* **Host-Local UDS Transport**: Eliminates TCP stack overhead with binary-framed Unix Domain Sockets featuring bounded memory queues, request multiplexing, and deterministic backpressure.

---

## Architecture

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

### Architectural Principles

* **Layered Concurrency**: A single accept loop feeds independent per-connection pipelines. Requests are decoded concurrently while responses are dispatched via a single-writer queue, ensuring frame ordering and deterministic backpressure.
* **Synchronous Client Boundary**: The client library provides a clean, blocking interface optimized for embedding into database engines (e.g. C/FFI, PostgreSQL backends, JNI) without requiring a client-side async event loop.
* **Credential-Agnostic Physical Deduplication**: Cache residency is keyed by the physical identity `(BackendDataIdentity, bucket, key)` rather than transient client credentials. Multiple sessions or tenants accessing the same underlying data automatically share resident cache bytes without re-authentication.

---

## Core Capabilities & Key Features

### 1. Zero-IPC Direct I/O via FD Passing (`SCM_RIGHTS`)
* **Descriptor Passing on Hit**: When opening an object that is already completely cached on local disk (`CompleteFile`), the server sends the open OS file descriptor across the Unix domain socket using `SCM_RIGHTS` ancillary messages.
* **Kernel-Direct `pread`**: The client switches to direct read mode (`ReadPath::Direct`), issuing `pread` syscalls straight against the local file descriptor.
* **Zero Overhead**: Read operations bypass the daemon process, socket IPC buffers, and memory copying, achieving bare-metal page cache and NVMe throughput.

### 2. Two-Tier Caching & Large Object Streaming
* **Small-Object KV Store**: Objects at or below the configured threshold (default $\le 4\text{ KiB}$) are stored directly in an embedded `redb` KV store alongside metadata, eliminating filesystem block fragmentation and small-file inode exhaustion.
* **Concurrent Chunk Fill with Single-Flight Election**: Large objects are fetched in configurable chunks (default $32\text{ MiB}$). When multiple concurrent queries request the same cold object, a single leader downloads from the remote backend while follower requests stream directly from the local partial file as chunks land.
* **Zero-Probe Read Handle**: Opening a handle freezes its residency. Subsequent reads on the same handle execute against in-memory KV slices or direct file descriptors without querying the index.

### 3. Path-Handoff Transaction Staging Writes
* **Direct Local Appends**: Database transactions create staging files via `StagingFile::create` with `O_CREAT | O_EXCL | O_APPEND` and write data locally using standard filesystem syscalls.
* **Zero-State Server Handoff**: The storage daemon maintains no in-memory buffers or open state during local writes. When the file is closed, a single `upload(bucket, key)` request instructs the server to stream the staged file to object storage.
* **Caller-Owned Lifecycle**: Transaction aborts and cleanup are handled directly by the database through the local filesystem, avoiding complex distributed rollback RPCs.

### 4. Backpressure & Memory Guardrails
* **Dual-Bounded Response Queues**: Response channels enforce both frame-count and byte-budget limits, preventing fast storage readers from overwhelming server memory when serving slow consumers.
* **Pre-Allocated Read Budgets**: Mediated read operations reserve buffer capacity prior to disk reads, preventing sudden memory spikes under high concurrency.
* **Orderly Draining & Timeouts**: Connection state machines gracefully drain in-flight requests on client half-close (`EOF`) and automatically disconnect stalled peers.

### 5. Watermark & Event-Driven Eviction
* **Allocation-Free Write Nudges**: Every cache admission triggers a zero-allocation watermark check. If resident bytes exceed the start watermark, LRU eviction immediately reclaims space down to the target watermark.
* **Periodic Janitor**: An asynchronous background task reclaims orphan partial files and rebalances storage budgets during idle periods.
* **Active Handle Protection**: Objects with active open read handles are immune to eviction, guaranteeing reading queries uninterrupted access.

---

## Data Flow & Lifecycle

### Direct Read Flow (Cache Hit vs. Miss)

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

### Staging Write Flow (Transactional Path-Handoff)

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

---

## Documentation

For an in-depth exploration of the system design, state machines, and consistency invariants, see:

* [docs/design.md](docs/design.md) — Comprehensive design rationale, invariant guarantees, and recovery model.

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
