# Debug Logging

Jetliner uses `tracing` for debug logging.

## Usage

**Python** (via `JETLINER_LOG`):
```bash
JETLINER_LOG=debug python script.py   # Full debug output
JETLINER_LOG=info python script.py    # Info and above
JETLINER_LOG=warn python script.py    # Warnings only (default)
```

**Rust tests** (via `RUST_LOG`):
```bash
cargo test                                              # Warnings only (default)
RUST_LOG=debug cargo test -- --nocapture                # Full debug output
RUST_LOG=jetliner::reader=debug cargo test -- --nocapture  # Debug for reader module only
```

Output goes to **stderr**, keeping it separate from program output.

## How it works

1. **`tracing`** — Provides logging macros (`debug!`, `info!`, `warn!`). Zero-cost when no subscriber is attached.

2. **`tracing-subscriber`** — Collects and outputs logs. Two separate initializations:
   - **Python**: In `#[pymodule]` init, reads `JETLINER_LOG`
   - **Rust tests**: Via `ctor` (runs before any test), reads `RUST_LOG`

3. **Default behavior** — With no env var set, level is `warn`. Debug/info logs are filtered out with near-zero overhead.

## What's logged

| Level | What |
|-------|------|
| info | Reader opening, S3 object opened |
| debug | Block parsing, decompression stats, S3 requests, schema parsing |
| warn | Skipped blocks, sync marker recovery, decompression failures |

## Design choices

- **stderr not stdout** — Logs don't interfere with program output; scripts can pipe stdout cleanly
- **No Python logging bridge** — `pyo3-log` requires GIL acquisition per log call, which blocks the async runtime and risks deadlocks
- **`warn` default** — Silent in normal operation; users opt-in to debug output
