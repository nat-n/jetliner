# PyO3 extension-module Feature and Testing

## Context

Task 9.1 (Avro to Arrow type mapping) required running unit tests in `src/convert/arrow.rs`. Initial attempts to run `cargo test` failed with linker errors about undefined Python symbols (`_PyObject_GetAttr`, `_Py_InitializeEx`, etc.).

## Problem

PyO3's `extension-module` feature tells the linker that Python symbols will be provided at runtime by the embedding Python interpreter. This works for `.so`/`.dylib` files loaded by Python, but breaks standalone test binaries which have no Python interpreter to provide those symbols.

## Decision

Make `extension-module` an optional Cargo feature instead of always-on:

```toml
# Cargo.toml
[features]
extension-module = ["pyo3/extension-module"]

[dependencies]
pyo3 = { version = "0.26", features = ["auto-initialize"] }
```

```toml
# pyproject.toml
[tool.maturin]
features = ["extension-module"]
```

## Outcome

- `cargo test` works without `extension-module`, using `auto-initialize` to create a Python interpreter when needed
- `maturin develop/build` enables `extension-module` via pyproject.toml, producing a proper Python extension
- Both workflows coexist without conflict

## References

- PyO3 docs: https://pyo3.rs/v0.26.0/building-and-distribution#the-extension-module-feature
- Related task: 9.1 (Implement Avro to Arrow type mapping)
