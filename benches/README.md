# rocksmap Benchmarks

Criterion-based benchmarks for rocksmap, organized into two categories. These measure the
performance of the operations as currently implemented; areas still being reworked (key
ordering, prefix/range scans) may change these numbers and benchmarks.

## Structure

```
benches/
├── unit/                    # Quick, focused benchmarks
│   ├── basic_ops.rs        # Basic CRUD operations
│   ├── batch_ops.rs        # Batch operation performance
│   └── iterators.rs        # Iterator performance
├── integration/            # Comprehensive, realistic benchmarks
│   ├── read_heavy.rs       # Read-intensive workloads
│   ├── write_heavy.rs      # Write-intensive workloads
│   └── mixed_workload.rs   # Mixed read/write scenarios
├── lib.rs                  # Common utilities and helpers
└── README.md
```

## Benchmark Categories

Dataset sizes come from the `config` constants in [lib.rs](lib.rs):
`UNIT_TEST_SIZE = 1_000`, `INTEGRATION_TEST_SIZE = 10_000`, `STRESS_TEST_SIZE = 100_000`.

### Unit Benchmarks (`unit/`)

- **Purpose**: Quick feedback during development.
- **Dataset Size**: Small, on the order of `UNIT_TEST_SIZE` (1K) items.
- **Use Cases**: local development and quick regression checks.

### Integration Benchmarks (`integration/`)

- **Purpose**: Performance validation on larger, more realistic workloads.
- **Dataset Size**: Larger, on the order of `INTEGRATION_TEST_SIZE` (10K) to
  `STRESS_TEST_SIZE` (100K) items.
- **Use Cases**: pre-release validation and load-pattern testing.

Execution time depends on your machine; run them locally to see actual durations.

## Running Benchmarks

### Quick Unit Tests (Development)

```bash
# Run all unit benchmarks
cargo bench --bench basic_ops
cargo bench --bench batch_ops  
cargo bench --bench iterators

# Run specific benchmark function
cargo bench --bench basic_ops -- benchmark_put_operations
```

### Comprehensive Integration Tests (Validation)

```bash
# Run all integration benchmarks
cargo bench --bench read_heavy
cargo bench --bench write_heavy
cargo bench --bench mixed_workload

# Run specific workload
cargo bench --bench mixed_workload -- benchmark_balanced_workload
```

### Run All Benchmarks

```bash
# Run everything (takes several minutes)
cargo bench
```

## Benchmark Descriptions

### Unit Benchmarks

#### `basic_ops.rs`

- **put_string**: Single key-value insertion performance
- **get_string**: Single key retrieval performance  
- **delete_string**: Single key deletion performance

#### `batch_ops.rs`

- **batch_put_10_items**: Small batch insertion (10 items)
- **batch_put_50_items**: Medium batch insertion (50 items)
- **batch_mixed_operations**: Mixed batch operations (puts, updates)

#### `iterators.rs`

- **iterate_1000_items**: Full iteration over 1K items
- **iterate_with_prefix**: Prefix-based iteration
- **iterate_range_100_items**: Range-based iteration

### Integration Benchmarks

#### `read_heavy.rs`

- **sequential_reads_10k**: Sequential read patterns
- **random_reads_50k_dataset**: Random access patterns
- **range_scan_1000_items**: Range scanning performance
- **concurrent_reads_4_threads**: Multi-threaded read performance

#### `write_heavy.rs`

- **sequential_writes_10k**: Sequential write patterns
- **large_batch_1000_items**: Large batch operations (1K items)
- **large_batch_5000_items**: Large batch operations (5K items)
- **concurrent_writes_4_threads**: Multi-threaded write performance
- **update_heavy_1000_updates**: Update-intensive workloads
- **mixed_write_operations**: Mixed insert/update/delete operations

#### `mixed_workload.rs`

- **balanced_read_write_70_30**: 70% reads, 30% writes
- **read_heavy_90_10**: 90% reads, 10% writes
- **write_heavy_30_70**: 30% reads, 70% writes
- **concurrent_mixed_4_threads**: Multi-threaded mixed operations
- **realistic_app_pattern**: Simulates real application usage patterns

## Performance Baselines

When running benchmarks, Criterion will automatically:

- Detect performance regressions
- Show statistical confidence intervals
- Compare against previous runs
- Generate detailed HTML reports (in `target/criterion/`)

## CI integration (not yet configured)

There is currently no CI in this repository. The snippets below are illustrative examples of
how these benchmarks could be wired into a workflow if one is added later:

```yaml
# Example only — no workflow is configured yet.
- name: Run unit benchmarks
  run: |
    cargo bench --bench basic_ops -- --quick
    cargo bench --bench batch_ops -- --quick

- name: Run full benchmark suite
  run: cargo bench
```

## Interpreting Results

Benchmark output shows:

- **Time**: Mean execution time with confidence intervals
- **Change**: Performance change from previous runs
- **Throughput**: Operations per second (when applicable)

Example output:

```
benchmark_put_operations/put_string
                        time:   [1.2345 µs 1.2456 µs 1.2567 µs]
                        change: [-2.5000% +0.0000% +2.5000%] (p = 0.50 > 0.05)
                        No change in performance detected.
```

## Adding New Benchmarks

1. **Unit benchmarks**: Add to appropriate file in `unit/`
2. **Integration benchmarks**: Add to appropriate file in `integration/`
3. **Common utilities**: Add to `lib.rs`
4. **Update this README**: Document new benchmarks

## Best Practices

- Use `black_box()` to prevent compiler optimizations
- Pre-populate databases for read benchmarks
- Use realistic data patterns and sizes
- Include both sequential and random access patterns
- Test concurrent scenarios when applicable
- Document expected performance characteristics
