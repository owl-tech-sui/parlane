# Changelog

## [0.1.0] - 2026-02-07

### Added
- Core API: `pmap`, `pfilter`, `pfor`, `pstarmap`
- Automatic GIL detection (`is_gil_disabled()`, `recommended_backend()`)
- Backend auto-selection: threads on GIL-free Python, processes otherwise
- Error strategies: `raise` (default), `skip`, `collect` (Ok/Err result types)
- Full type hints with `py.typed` (PEP 561)
- CI: GitHub Actions matrix (Python 3.10-3.13, Linux/macOS/Windows)

### Performance
- **Smart default workers**: thread backend defaults to `min(32, cpu+4, n_items)`,
  process backend defaults to `min(cpu, n_items)`. This matches or beats
  `concurrent.futures` defaults for both I/O-bound and CPU-bound workloads.
- **Fork context on Unix**: process backend uses `fork` start method on
  Unix/macOS, eliminating the `if __name__ == '__main__'` guard requirement.

### Benchmark Results (Apple M-series, 8 cores, Python 3.12)

#### I/O-bound: 100 tasks x 50ms sleep
| Method | Time | Speedup |
|--------|------|---------|
| Sequential | 5.35s | 1.0x |
| parlane `pmap` | 0.48s | **11.2x** |
| `concurrent.futures` | 0.49s | 11.0x |

#### CPU-bound: 200 tasks x heavy math
| Method | Time | Speedup |
|--------|------|---------|
| Sequential | 1.26s | 1.0x |
| parlane `pmap` | 0.28s | **4.5x** |
| `concurrent.futures` | 0.29s | 4.4x |

**parlane overhead: ~0ms** (within measurement noise)
