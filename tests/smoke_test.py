"""Minimal smoke test for release wheel validation."""
import os
import jetliner

# Support both docker mount (/tests/...) and direct run (tests/...)
if os.path.exists("/tests/data"):
    base = "/tests/data"
else:
    base = "tests/data"

df = jetliner.read_avro(f"{base}/apache-avro/weather.avro")
assert df.shape == (5, 3), f"Unexpected shape: {df.shape}"
assert df.columns == ["station", "time", "temp"], f"Unexpected columns: {df.columns}"
print(f"Smoke test passed: {df.shape[0]} rows, {df.columns}")
