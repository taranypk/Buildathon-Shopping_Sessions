Warning: fs was declared with const; use let for reassignable variables.
# Shopping Sessions

This submission reads the CSV inputs under `DATA_DIR` (default `/data`) and writes the exact daily Parquet outputs under `OUTPUT_DIR` (default `/output`). It targets the shopping-session rules in [`rules.md`](rules.md).

The pipeline uses a small C validator to enforce the LF-only CSV dialect and discard malformed rows, then Polars for parsing, grouping, sessionization, attribution, and Parquet output. Temporary normalized CSVs are written under `SCRATCH_DIR` (default `/scratch`). The image builds for `linux/amd64` and needs no network at runtime.

## Build and run

```sh
docker build --platform linux/amd64 -t shopping-sessions:latest .
docker run --rm --network none \
  -v ./data:/data:ro -v ./out:/output -v ./scratch:/scratch \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch \
  shopping-sessions:latest
```

The scorer runs the image without arguments. The pipeline writes `user_daily/` and `campaign_daily/`, each containing Parquet files with the required schema.
