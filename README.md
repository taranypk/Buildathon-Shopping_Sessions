# Shopping Sessions

This submission reads the CSV inputs under `DATA_DIR` (default `/data`) and writes the exact daily Parquet outputs under `OUTPUT_DIR` (default `/output`). It targets the shopping-session rules in [`rules.md`](rules.md).

The pipeline validates the LF-only CSV dialect, then uses Polars to deduplicate records and build sessions. Larger inputs are split by record ID for deduplication and by linked person for sessionization. Campaign attribution counts assists over merged click windows. Temporary partitions are written under `SCRATCH_DIR` (default `/scratch`). The image builds for `linux/amd64` and needs no network at runtime.

## Build and run

```sh
docker build --platform linux/amd64 -t shopping-sessions:latest .
docker run --rm --network none \
  -v ./data:/data:ro -v ./out:/output -v ./scratch:/scratch \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch \
  shopping-sessions:latest
```

The scorer runs the image without arguments. The pipeline writes `user_daily/` and `campaign_daily/`, each containing Parquet files with the required schema.
