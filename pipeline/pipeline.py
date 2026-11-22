#!/usr/bin/env python3
"""Run the native aggregation and encode the resulting daily rows as Parquet."""
import json
import os
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

if "NCPU" in os.environ:
    os.environ.setdefault("POLARS_MAX_THREADS", os.environ["NCPU"])
import numpy as np
import polars as pl

USER = np.dtype([
    ("revenue_cents", "<i8"), ("person", "<u4"), ("day", "<i4"),
    ("sessions", "<u4"), ("events", "<u4"), ("category", "<u4"), ("reserved", "<u4"),
])
METRICS = ["sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"]
DAY_MS = 86_400_000


def dictionary(path, name):
    if path.stat().st_size == 0:
        return pl.Series(name, [], dtype=pl.String)
    return pl.read_csv(path, has_header=False, schema={name: pl.String})[name]


def write_users(work, out):
    people = dictionary(work / "people.csv", "user_id")
    categories = dictionary(work / "categories.csv", "top_category")
    null_category = len(categories)
    categories = categories.append(pl.Series("top_category", [None], dtype=pl.String))
    files = sorted(work.glob("users-*.bin"))
    parts, count, part_number, total = [], 0, 0, 0
    for index, path in enumerate(files or [None]):
        rows = np.fromfile(path, dtype=USER) if path else np.empty(0, dtype=USER)
        category = np.where(rows["category"] == 0xffffffff, null_category, rows["category"])
        parts.append(pl.DataFrame({
            "user_id": people.gather(rows["person"]),
            "day": pl.Series(rows["day"]).cast(pl.Date),
            "sessions": rows["sessions"], "events": rows["events"],
            "revenue_cents": rows["revenue_cents"], "top_category": categories.gather(category),
        }))
        count += len(rows)
        if count >= 500_000 or index + 1 >= len(files):
            pl.concat(parts).write_parquet(out / f"part-{part_number:03}.parquet", compression="lz4")
            total += count
            parts, count = [], 0
            part_number += 1
    return total


def write_campaigns(work, out, first_day, days):
    ids = dictionary(work / "campaigns.csv", "campaign_id")
    channels = dictionary(work / "channels.csv", "channel")
    totals = np.fromfile(work / "campaign.bin", dtype="<i8").reshape(len(ids), days, 4)
    campaign, day = np.nonzero(totals.any(axis=2))
    rows = totals[campaign, day]
    daily = pl.DataFrame({
        "campaign_id": ids.gather(campaign), "channel": channels.gather(campaign),
        "day": pl.Series((day + first_day).astype(np.int32)).cast(pl.Date),
        **{name: rows[:, i] for i, name in enumerate(METRICS)},
    })
    daily.write_parquet(out / "part-0.parquet", compression="lz4")
    return daily.height


def main():
    started = time.perf_counter()
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
    manifest = json.loads((data / "manifest.json").read_text(encoding="utf-8"))
    start, end = (
        int(datetime.fromisoformat(manifest[key].replace("Z", "+00:00")).timestamp()) * 1000
        for key in ("window_start", "window_end")
    )
    user_dir, campaign_dir = output / "user_daily", output / "campaign_daily"
    for directory in (user_dir, campaign_dir):
        directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch) as folder:
        work = Path(folder)
        subprocess.run([
            "/usr/local/bin/shopping", data, work, str(start), str(end),
            os.environ.get("NCPU", str(min(8, len(os.sched_getaffinity(0))))),
        ], check=True)
        users = write_users(work, user_dir)
        first_day = start // DAY_MS
        campaigns = write_campaigns(work, campaign_dir, first_day, (end-1)//DAY_MS - first_day + 1)
    print(f"wrote {users} user-day rows and {campaigns} campaign-day rows")
    print(f"pipeline_seconds={time.perf_counter()-started:.6f}")


if __name__ == "__main__":
    main()
