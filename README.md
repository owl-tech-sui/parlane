# parlane

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
| Dependencies | **Zero** | numpy, etc. | stdlib |
| Type hints | **Full (py.typed)** | Partial | Partial |

## Install

```bash
pip install parlane
```

Requires Python 3.10+. Zero dependencies.

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

## API

### `pmap(fn, items, **options) -> list`

Apply `fn` to each item in parallel. Returns results in order.

```python
results = pmap(process_image, images)
```

### `pfilter(fn, items, **options) -> list`

Keep items where `fn` returns `True`. Parallel evaluation.

```python
valid = pfilter(is_valid, records)
```

### `pfor(fn, items, **options) -> None`

Apply `fn` to each item for side effects.

```python
pfor(send_notification, users)
```

### `pstarmap(fn, items, **options) -> list`

Like `pmap`, but unpacks each item as arguments.

```python
results = pstarmap(pow, [(2, 10), (3, 5), (10, 3)])
# [1024, 243, 1000]
```

### Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `workers` | `int` | CPU count | Number of parallel workers |
| `backend` | `str` | `"auto"` | `"auto"`, `"thread"`, or `"process"` |
| `timeout` | `float` | `None` | Per-task timeout in seconds |
| `chunksize` | `int` | `None` | Chunk size (process backend) |
| `on_error` | `str` | `"raise"` | `"raise"`, `"skip"`, or `"collect"` |

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
3. **Execute** with the chosen backend
4. **Return results** in input order

Users can override with `backend="thread"` or `backend="process"`.

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
```

## License

MIT
