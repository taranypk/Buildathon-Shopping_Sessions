#!/usr/bin/env python3
"""Read the run manifest and start the compiled aggregator."""
import json
import os
from datetime import datetime
from pathlib import Path


def main():
    data = Path(os.environ.get("DATA_DIR", "/data"))
    manifest = json.loads((data / "manifest.json").read_text(encoding="utf-8"))
    bounds = [
        str(int(datetime.fromisoformat(manifest[key].replace("Z", "+00:00")).timestamp()) * 1000)
        for key in ("window_start", "window_end")
    ]
    binary = "/usr/local/bin/shopping"
    workers = os.environ.get("NCPU", str(min(8, len(os.sched_getaffinity(0)))))
    os.execv(binary, [binary, str(data), os.environ.get("OUTPUT_DIR", "/output"), *bounds, workers])


if __name__ == "__main__":
    main()
