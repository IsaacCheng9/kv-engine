# KV Engine

[![Test](https://github.com/IsaacCheng9/kv-engine/actions/workflows/test.yml/badge.svg)](https://github.com/IsaacCheng9/kv-engine/actions/workflows/test.yml)

A key-value storage engine in C++20. Currently an in-memory store, with plans to
add LSM-tree architecture (write-ahead logging, SSTables, compaction).

## Building

```bash
cmake -B build -DSANITISE=ON
cmake --build build
```

## Testing

```bash
cd build && ctest --output-on-failure
```
