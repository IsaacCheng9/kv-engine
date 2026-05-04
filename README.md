# KV Engine

[![Test](https://github.com/IsaacCheng9/kv-engine/actions/workflows/test.yml/badge.svg)](https://github.com/IsaacCheng9/kv-engine/actions/workflows/test.yml)

A crash-safe, concurrent key-value storage engine in C++23 with an LSM-tree
design.

## Key Features

- **Write-ahead logging** – crash recovery via WAL replay with CRC32 integrity
  checks and length-prefix framing
- **Sorted in-memory memtable** – `std::map`-backed structure with
  `std::shared_mutex` for concurrent reads and exclusive writes
- **SSTable persistence** – sorted, immutable on-disk files with index blocks
  and footer for efficient point lookups
- **Multi-level reads** – memtable first, then SSTables from newest to oldest
  with first match winning and tombstone semantics for deletes
- **Levelled compaction** – background thread merges L0 SSTables into L1 with
  fine-grained locking, so reads and flushes continue during compaction
- **SSTable reader cache** – parsed readers (index + file descriptor) stay
  resident for each file's lifetime, and `pread`-based positioned reads make
  them safe to share across concurrent `get()` callers; eliminates the open +
  footer + index parse that would otherwise happen on every lookup
- **Per-SSTable Bloom filter** – probabilistic membership test built during
  `finalise()` and stored in a new block between the index and footer; on
  `get()`, the filter is consulted before the binary search to short-circuit
  keys guaranteed not to be in the file (no false negatives, ~1% false positive
  rate)
- **Key range pruning** – each cached reader's min/max keys are stored at the
  engine level so `get()` can skip SSTables whose key range cannot contain the
  lookup key, avoiding the Bloom check and binary search entirely for
  out-of-range files
- **gRPC API** – `Put` / `Get` / `Delete` over unary RPCs and `Scan` as
  server-streaming for range scans; snapshot semantics so concurrent writes,
  flushes, and compactions don't perturb in-flight scans

### Planned Features

- gRPC API layer for remote client access
- Raft consensus for distributed replication across multiple nodes

## Architecture

```mermaid
flowchart TD
    client[Client] -->|Put / Get / Delete / Scan| grpc[gRPC Server]
    grpc -->|engine API| engine[Engine]
    engine -->|writes| wal[Write-Ahead Log]
    engine -->|writes / reads| memtable[Memtable<br/>sorted in-memory]
    memtable -->|flush when full| l0[L0 SSTables<br/>overlapping key ranges]
    l0 -->|background compaction| l1[L1 SSTables<br/>merged, deduplicated]
    engine -.reads.-> l0
    engine -.reads.-> l1
    wal -.replay on startup.-> memtable
```

## Build

Requires a C++23 compiler with `<print>` support – GCC 14+, a Clang toolchain
with libc++ 19+, or Apple Clang 16+ (Xcode 16+).

Also requires gRPC and Protobuf:

- macOS: `brew install grpc protobuf`
- Ubuntu/Debian: `sudo apt-get install libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc`

```bash
cmake -B build -DSANITISE=ON
cmake --build build
```

## Run Tests

```bash
cd build && ctest --output-on-failure
```

The default `-DSANITISE=ON` build enables **AddressSanitizer** (catches
use-after-free, buffer overflows, leaks) and **UndefinedBehaviorSanitizer**
(catches signed overflow, null dereferences, etc.).

### ThreadSanitizer

ASan and TSan can't be enabled at the same time, so TSan gets its own build
directory. Use it whenever changing code that runs across multiple threads
(background compaction, concurrent writes, locking):

```bash
cmake -B build_tsan -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer -g" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build build_tsan
cd build_tsan && ctest --output-on-failure
```

Single-threaded tests pass green even when there are races that only appear
under real contention - run the concurrency tests under TSan to flush those out.
Both ASan and TSan builds run on every push in CI.

## Benchmarks

Benchmarks are built into a separate binary via an opt-in CMake flag, using a
release build with `-O3 -DNDEBUG` so sanitiser overhead doesn't skew the
numbers.

```bash
cmake -B build_bench -DBENCHMARK=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build_bench
./build_bench/kv_engine_benchmark
```

To save the results to a file while still seeing progress in the terminal:

```bash
cmake -B build_bench -DBENCHMARK=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build_bench
./build_bench/kv_engine_benchmark | tee docs/YYYY_MM_DD_label.txt
```

Use a date-prefixed filename and a label describing the milestone (e.g.
`2026_04_13_baseline.txt`, `2026_04_20_post_bloom_filter.txt`). Results are
stored as `.txt` so the log lines render as-is; the markdown table in the output
can still be copy-pasted into PR descriptions for feature-level improvement
comparisons.

### Scenarios

- **put** – pure write throughput on a small memtable (forces frequent flushes).
  Dominated by `fsync` cost on the WAL
- **get_memtable** – reads served entirely from the memtable (no disk I/O).
  Best-case read path
- **get_sstable** – reads served from SSTables on disk. Each level is scanned
  from newest to oldest; key range pruning skips files that can't contain the
  key, the Bloom filter short-circuits files whose filter rules the key out, and
  an in-memory binary search locates the entry in the remaining candidates
- **get_miss** – negative lookups for keys that were never inserted. Queries use
  indices past the inserted range (`key_000000250000`...) so they share the
  `"key_000000"` prefix with stored keys, exercising key range pruning and the
  Bloom filter against a realistic miss workload rather than trivially-rejected
  keys
- **mixed_50_50** – 50% reads / 50% writes with deterministic key selection.
  Production-like workload
- **crash_recovery** – time to replay a populated WAL on engine startup. Each op
  populates a fresh WAL, destroys the engine, and times the reopen. Measures the
  cost of the durability guarantee after a simulated crash
- **grpc_put** / **grpc_get_memtable** – same workloads as `put` and
  `get_memtable` but issued through the gRPC client to a loopback server.
  Difference vs the direct-call rows is the gRPC + serialisation tax
