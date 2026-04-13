# KV Engine

[![Test](https://github.com/IsaacCheng9/kv-engine/actions/workflows/test.yml/badge.svg)](https://github.com/IsaacCheng9/kv-engine/actions/workflows/test.yml)

A key-value storage engine in C++20 with LSM-tree architecture.

## Key Features

- **Write-ahead logging** – crash recovery via WAL replay with CRC32 integrity
  checks and length-prefix framing
- **Sorted in-memory memtable** – `std::map`-backed structure with
  `std::shared_mutex` for concurrent reads and exclusive writes
- **SSTable persistence** – sorted, immutable on-disk files with index blocks
  and footer for efficient point lookups
- **Multi-level reads** – memtable first, then SSTables from newest to oldest
  with first match winning and tombstone semantics for deletes

### Planned Features

- Levelled compaction with background thread and fine-grained locking
- Bloom filters per SSTable to skip unnecessary disk reads on negative lookups
- gRPC API layer for remote client access
- Raft consensus for distributed replication across multiple nodes

## Architecture

```mermaid
flowchart TD
    client[Client] -->|Put / Get / Delete| engine[Engine]
    engine -->|writes| wal[Write-Ahead Log]
    engine -->|writes / reads| memtable[Memtable<br/>sorted in-memory]
    memtable -->|flush when full| sstables[SSTables<br/>sorted, immutable on disk]
    engine -.reads.-> sstables
    wal -.replay on startup.-> memtable
```

## Build

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
- **get_sstable** – reads served from SSTables on disk. Degrades quadratically
  without levelled compaction since each lookup scans all SSTables
- **get_miss** – negative lookups for keys that were never inserted. Worst case
  without bloom filters – every SSTable must be checked
- **mixed_50_50** – 50% reads / 50% writes with deterministic key selection.
  Production-like workload
- **crash_recovery** – time to replay a populated WAL on engine startup. Each op
  populates a fresh WAL, destroys the engine, and times the reopen. Measures the
  cost of the durability guarantee after a simulated crash
