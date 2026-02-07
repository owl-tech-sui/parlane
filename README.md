# parlane

[![PyPI](https://img.shields.io/pypi/v/parlane)](https://pypi.org/project/parlane/)
[![Python](https://img.shields.io/pypi/pyversions/parlane)](https://pypi.org/project/parlane/)
[![Tests](https://github.com/owl-tech-sui/parlane/actions/workflows/tests.yml/badge.svg)](https://github.com/owl-tech-sui/parlane/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Dead-simple parallel data processing for Python.**

parlane gives you parallel `map`, `filter`, and `for-each` in one line.
On GIL-free Python (3.13t+), it uses threads automatically.
On standard Python, it falls back to processes. You don't need to think about it.

```python
from parlane import pmap

results = pmap(process, items)  # That's it.
```

## Why parlane?

| | parlane | joblib | concurrent.futures |
|---|---|---|---|
| Lines of code | **1** | 1 (but 3 concepts) | 3-4 |
| GIL-aware | **Auto** | No | No |
| Async support | **Built-in** | No | Manual |
| Progress bar | **Built-in** | No | Manual |
| Pipeline API | **Built-in** | No | No |
| Dependencies | **Zero** (core) | numpy, etc. | stdlib |
| Type hints | **Full (py.typed)** | Partial | Partial |
| `__main__` guard | **Not needed** | Not needed | Required (macOS/Win) |

## Install

```bash
pip install parlane

# With progress bar support
pip install parlane[progress]
```

Requires Python 3.10+. Zero core dependencies.

## Quick Start

```python
from parlane import pmap, pfilter, pfor

# Parallel map
results = pmap(lambda x: x ** 2, range(1000))

# Parallel filter
evens = pfilter(lambda x: x % 2 == 0, range(1000))

# Parallel for-each (side effects)
pfor(save_to_db, records)

# With options
results = pmap(fetch, urls, workers=16, backend="thread", timeout=30.0)
```

## Progress Bar

Add real-time progress display with a single parameter. Requires `tqdm` (`pip install parlane[progress]`).

```python
from parlane import pmap, pfilter

# Enable with description
results = pmap(process, images, backend="thread", progress="Processing")
# Processing: 100%|██████████| 500/500 [00:03<00:00, 160.2it/s]

# Enable without description
results = pmap(process, images, progress=True)

# Works with all sync functions
pfilter(is_valid, records, progress="Validating")
```

No progress overhead when `progress=False` (default) — the fast `executor.map()` path is preserved.

## Async API

Native async support for I/O-bound workloads. Uses `asyncio.Semaphore` for concurrency control — no executor needed.

```python
import asyncio
from parlane import apmap, apfilter, apfor

async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return await resp.text()

# Async parallel map
pages = await apmap(fetch, urls, workers=20)

# Async parallel filter
async def is_alive(url):
    ...  # return True/False

alive = await apfilter(is_alive, urls, workers=10)

# Async for-each
await apfor(send_notification, users, workers=5)

# With progress
pages = await apmap(fetch, urls, workers=20, progress="Fetching")
```

## Pipeline API

Chain operations fluently with lazy evaluation. Nothing executes until a terminal method is called.

```python
from parlane import pipeline

# Lazy chain — executes on .collect()
results = (
    pipeline(raw_data)
    .map(parse)
    .filter(is_valid)
    .map(transform)
    .collect()
)

# With progress
results = (
    pipeline(images)
    .progress("ETL")
    .map(resize)
    .filter(has_face)
    .map(classify)
    .collect()
)

# Flat map + batch
words = pipeline(documents).flat_map(tokenize).batch(100).map(embed).collect()

# Terminal methods
pipeline(items).map(fn).count()          # -> int
pipeline(items).map(fn).first()          # -> T | None
pipeline(items).map(fn).reduce(sum)      # -> R

# Configuration
pipeline(items).workers(8).backend("thread").on_error("skip").map(fn).collect()
```

Pipelines are **immutable** — each method returns a new pipeline, so the original can be reused:

```python
base = pipeline(data).map(normalize)
train = base.filter(is_train).collect()
test  = base.filter(is_test).collect()
```

## Benchmarks

Measured on Apple M-series (8 cores), Python 3.12:

### I/O-bound: 100 tasks x 50ms sleep

| Method | Time | Speedup |
|--------|------|---------|
| Sequential `for` loop | 5.35s | 1.0x |
| **parlane** `pmap` | **0.48s** | **11.2x** |
| `concurrent.futures` | 0.49s | 11.0x |

### CPU-bound: 200 tasks x heavy math

| Method | Time | Speedup |
|--------|------|---------|
| Sequential `for` loop | 1.26s | 1.0x |
| **parlane** `pmap` | **0.28s** | **4.5x** |
| `concurrent.futures` | 0.29s | 4.4x |

**Zero overhead.** parlane uses `concurrent.futures` under the hood with smart defaults
that match or beat manual configuration.

Run benchmarks yourself:

```bash
python benchmarks/bench_vs_stdlib.py
```

## API Reference

### Sync Functions

#### `pmap(fn, items, **options) -> list`

Apply `fn` to each item in parallel. Returns results in order.

```python
results = pmap(process_image, images)
```

#### `pfilter(fn, items, **options) -> list`

Keep items where `fn` returns `True`. Parallel evaluation.

```python
valid = pfilter(is_valid, records)
```

#### `pfor(fn, items, **options) -> None`

Apply `fn` to each item for side effects.

```python
pfor(send_notification, users)
```

#### `pstarmap(fn, items, **options) -> list`

Like `pmap`, but unpacks each item as arguments.

```python
results = pstarmap(pow, [(2, 10), (3, 5), (10, 3)])
# [1024, 243, 1000]
```

### Async Functions

#### `await apmap(fn, items, **options) -> list`

Async parallel map with semaphore-based concurrency control.

#### `await apfilter(fn, items, **options) -> list`

Async parallel filter.

#### `await apfor(fn, items, **options) -> None`

Async parallel for-each.

### Options

#### Sync options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `workers` | `int` | auto | Thread: `cpu+4`, Process: `cpu` (capped at item count) |
| `backend` | `str` | `"auto"` | `"auto"`, `"thread"`, or `"process"` |
| `timeout` | `float` | `None` | Per-task timeout in seconds |
| `chunksize` | `int` | `None` | Chunk size (process backend) |
| `on_error` | `str` | `"raise"` | `"raise"`, `"skip"`, or `"collect"` |
| `progress` | `bool \| str` | `False` | `True`, `False`, or description string |

#### Async options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `workers` | `int` | auto | Max concurrent tasks (capped at 32) |
| `on_error` | `str` | `"raise"` | `"raise"`, `"skip"`, or `"collect"` |
| `progress` | `bool \| str` | `False` | `True`, `False`, or description string |

### Error Handling

```python
# Default: raise on first error
pmap(risky_fn, items)  # raises immediately

# Skip errors silently
results = pmap(risky_fn, items, on_error="skip")

# Collect all results (Ok/Err)
results = pmap(risky_fn, items, on_error="collect")
for r in results:
    if r.is_ok():
        print(r.unwrap())
    else:
        print(f"Error: {r.exception}")
```

Error handling works the same way in async functions:

```python
results = await apmap(risky_fn, items, on_error="collect")
```

### GIL Detection

```python
from parlane import is_gil_disabled, recommended_backend

print(is_gil_disabled())      # True on 3.13t+, False otherwise
print(recommended_backend())  # "thread" or "process"
```

## How It Works

1. **Detect GIL state** at import time (cached)
2. **Choose backend** automatically:
   - GIL disabled -> `ThreadPoolExecutor` (true parallelism, no serialization overhead)
   - GIL enabled -> `ProcessPoolExecutor` (bypass GIL via multiprocessing)
3. **Pick optimal worker count**: threads get `cpu+4`, processes get `cpu` (never more than items)
4. **Execute** with the chosen backend
5. **Return results** in input order

Users can override with `backend="thread"` or `backend="process"`.

For async functions, `asyncio.Semaphore` controls concurrency directly — no executor needed.

## Development

```bash
git clone https://github.com/owl-tech-sui/parlane
cd parlane
pip install -e ".[dev]"

# Run tests
pytest -v

# Lint
ruff check src/ tests/
ruff format --check src/ tests/

# Type check
mypy src/parlane/ --strict

# Benchmarks
python benchmarks/bench_vs_stdlib.py
```

## License

MIT
