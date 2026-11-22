#!/usr/bin/env python3
"""Clean, partition, and aggregate the shopping-session logs."""
import ctypes
import json
import os
import resource
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

NCPU = os.environ.get("NCPU")
if NCPU:
    os.environ.setdefault("POLARS_MAX_THREADS", NCPU)

import numpy as np
import polars as pl

DATA = Path(os.environ.get("DATA_DIR", "/data"))
OUT = Path(os.environ.get("OUTPUT_DIR", "/output"))
SCRATCH = Path(os.environ.get("SCRATCH_DIR", "/scratch"))

EVENT_COLS = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
CLICK_COLS = ["click_id", "user_id", "campaign_id", "ts"]
LINK_COLS = ["user_id_a", "user_id_b"]
EVENT_SCHEMA = {"person": pl.String, "event_ms": pl.Int64, "product_id": pl.String, "price": pl.Int64}
CLICK_SCHEMA = {"person": pl.String, "click_id": pl.String, "click_ms": pl.Int64, "campaign": pl.UInt32}
CAMPAIGN_METRICS = ["sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"]

THIRTY_MIN = 1_800_000
DAY_MS = 86_400_000
SEVEN_DAYS = 7 * DAY_MS
PARTITION_BYTES = 128 * 1024**2
MAX_PARTITIONS = 32


def normalize_csvs(files, columns, prefix, buckets=1):
    """Validate the CSV dialect and hash its first field into scratch files."""
    subprocess.run(
        ["/usr/local/bin/csv_filter", str(len(columns)), ",".join(columns),
         prefix, str(buckets), "0", *files],
        check=True,
    )
    return [prefix.with_name(f"{prefix.name}-{i:03}.csv") for i in range(buckets)]


def read_csv(path, columns):
    result = pl.read_csv(
        path, schema={c: pl.String for c in columns}, null_values="", raise_if_empty=False
    )
    path.unlink()
    return result


def read_input(files, columns, prefix):
    return read_csv(normalize_csvs(files, columns, prefix)[0], columns)


def epoch_ms(name):
    value = pl.col(name)
    digits = value.str.contains(r"^[0-9]+$")
    # The datetime parser accepts leap seconds, but the input rules reject :60.
    iso = value.str.contains(
        r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
        r"(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$"
    )
    parsed = value.str.to_datetime(format="%+", time_unit="ms", time_zone="UTC", strict=False)
    return (
        pl.when(digits).then(value.cast(pl.Int64, strict=False))
        .when(iso).then(parsed.cast(pl.Int64))
        .otherwise(None)
    )


def utc_ingest_ms():
    return pl.col("ingest_ts").str.to_datetime(
        format="%Y-%m-%dT%H:%M:%SZ", time_unit="ms", strict=False
    ).cast(pl.Int64)


def present(name):
    return pl.col(name).is_not_null() & (pl.col(name) != "")


class Components:
    """Disjoint set whose representative is the byte-wise smallest ID."""

    def __init__(self):
        self.parent = {}

    def find(self, name):
        if name not in self.parent:
            self.parent[name] = name
            return name
        root = name
        while self.parent[root] != root:
            root = self.parent[root]
        while name != root:
            previous = self.parent[name]
            self.parent[name] = root
            name = previous
        return root

    def join(self, a, b):
        first, second = self.find(a), self.find(b)
        if first != second:
            self.parent[max(first, second)] = min(first, second)

    def mapping_frame(self):
        aliases, people = [], []
        for name in self.parent:
            root = self.find(name)
            if root != name:
                aliases.append(name)
                people.append(root)
        return pl.DataFrame(
            {"user_id": aliases, "person": people},
            schema={"user_id": pl.String, "person": pl.String},
        )


def linked_users(links):
    components = Components()
    for a, b in links.iter_rows():
        if a and b:
            components.join(a, b)
    return components.mapping_frame()


def map_person(frame, user_map):
    return frame.join(user_map, on="user_id", how="left").with_columns(
        pl.coalesce("person", "user_id").alias("person")
    )


def clean_events(events, user_map, start_ms, end_ms):
    # Every copy is in this bucket. Ingest timestamps are valid UTC text, so
    # byte order is time order. Only parse and validate the winning copy.
    events = (
        events.filter(present("event_id"))
        .sort("event_id", "ingest_ts")
        .unique(subset="event_id", keep="first")
        .drop("event_id")
    )
    purchase = pl.col("event_type") == "purchase"
    eligible = (
        events.filter(
            present("user_id") & present("product_id")
            & pl.col("event_type").is_in(["view", "cart", "purchase"])
        )
        .with_columns(
            epoch_ms("ts").alias("event_ms"),
            utc_ingest_ms().alias("ingest_ms"),
            pl.col("price_cents").cast(pl.Int64, strict=False).alias("price"),
        )
        .filter(
            pl.col("event_ms").is_between(start_ms, end_ms, closed="left")
            & (pl.col("ingest_ms") - pl.col("event_ms") <= DAY_MS)
            & (~purchase | (
                pl.col("price_cents").str.contains(r"^[0-9]+$")
                & pl.col("price").is_between(1, 10_000_000)
            ))
        )
        .select(
            "user_id", "event_ms",
            pl.when(purchase).then(pl.col("product_id")).alias("product_id"),
            pl.when(purchase).then(pl.col("price")).otherwise(0).alias("price"),
        )
    )
    return map_person(eligible, user_map).select(*EVENT_SCHEMA)


def clean_clicks(clicks, user_map, campaign_ids, start_ms, end_ms):
    eligible = (
        clicks.filter(present("click_id"))
        .unique(subset="click_id", keep="first")
        .filter(present("user_id"))
        .with_columns(epoch_ms("ts").alias("click_ms"))
        .filter(pl.col("click_ms").is_between(start_ms - SEVEN_DAYS, end_ms, closed="left"))
        .join(campaign_ids, on="campaign_id", how="inner")
    )
    return map_person(eligible, user_map).select(*CLICK_SCHEMA)


def repartition(frame, prefix, partition_count):
    frame = frame.with_columns(
        (pl.col("person").hash(seed=0) % partition_count).alias("bucket")
    )
    for (bucket,), part in frame.partition_by("bucket", as_dict=True, include_key=False).items():
        part.write_ipc(prefix.with_name(f"{prefix.name}-{bucket:03}.arrow"))


def read_partition(folder, name, index, schema):
    files = sorted(folder.glob(f"{name}-*-{index:03}.arrow"))
    if not files:
        return pl.DataFrame(schema=schema)
    return pl.concat([pl.read_ipc(file) for file in files], rechunk=False)


def sessionize(events):
    events = events.sort("person", "event_ms")
    new_session = (
        (pl.col("person") != pl.col("person").shift()).fill_null(True)
        | (pl.col("event_ms").diff() > THIRTY_MIN).fill_null(False)
    )
    return events.with_columns(
        pl.when(new_session).then(pl.col("event_ms")).forward_fill().alias("session_start")
    )


def session_day():
    return pl.from_epoch("session_start", time_unit="ms").dt.date().alias("day")


def summarize_users(events, products):
    events = sessionize(events)
    sessions = (
        events.group_by("person", "session_start")
        .agg(pl.len().alias("events"), pl.col("price").sum().alias("revenue_cents"))
        .with_columns(session_day())
    )
    categories = (
        events.filter(pl.col("price") > 0)
        .select("person", "product_id", "price", session_day())
        .join(products, on="product_id", how="left")
        .with_columns(pl.col("category").fill_null("unknown"))
        .group_by("person", "day", "category").agg(pl.col("price").sum())
        .sort(["person", "day", "price", "category"], descending=[False, False, True, False])
        .unique(subset=["person", "day"], keep="first")
        .select("person", "day", pl.col("category").alias("top_category"))
    )
    daily = (
        sessions.group_by("person", "day")
        .agg(pl.len().alias("sessions"), pl.col("events").sum(), pl.col("revenue_cents").sum())
        .join(categories, on=["person", "day"], how="left")
        .rename({"person": "user_id"})
    )
    return daily, sessions.select("person", "session_start", "revenue_cents")


def load_attribution():
    library = ctypes.CDLL("/usr/local/lib/libshopping_attribution.so")
    i64 = np.ctypeslib.ndpointer(dtype=np.int64, flags="C_CONTIGUOUS")
    u32 = np.ctypeslib.ndpointer(dtype=np.uint32, flags="C_CONTIGUOUS")
    attribute = library.attribute
    attribute.argtypes = [
        ctypes.c_size_t, u32, i64, i64, ctypes.c_size_t, u32, i64, u32,
        ctypes.c_uint32, ctypes.c_int64, ctypes.c_size_t, i64,
    ]
    attribute.restype = ctypes.c_int
    return attribute


def credit_campaigns(attribute, sessions, clicks, first_day, totals):
    if not sessions.height:
        return
    sessions = sessions.sort("person", "session_start").with_columns(
        pl.col("person").rle_id().cast(pl.UInt32).alias("person_index")
    )
    people = sessions.select("person", "person_index").unique(subset="person")
    clicks = (
        clicks.join(people, on="person", how="inner")
        .sort(["person_index", "click_ms", "click_id"], descending=[False, False, True])
    )
    result = attribute(
        sessions.height, sessions["person_index"].to_numpy(), sessions["session_start"].to_numpy(),
        sessions["revenue_cents"].to_numpy(),
        clicks.height, clicks["person_index"].to_numpy(), clicks["click_ms"].to_numpy(),
        clicks["campaign"].to_numpy(), totals.shape[0] - 1, first_day, totals.shape[1], totals,
    )
    if result:
        raise MemoryError("could not allocate campaign attribution state")


def write_campaigns(totals, first_day, campaigns):
    campaign, offset = np.nonzero(totals.any(axis=2))
    rows = totals[campaign, offset]
    all_campaigns = pl.concat(
        [campaigns, pl.DataFrame({"campaign_id": ["organic"], "channel": ["organic"]})]
    ).with_row_index("campaign")
    daily = pl.DataFrame({
        "campaign": campaign.astype(np.uint32),
        "day": pl.Series((offset + first_day).astype(np.int32), dtype=pl.Int32).cast(pl.Date),
        **{name: rows[:, i] for i, name in enumerate(CAMPAIGN_METRICS)},
    })
    daily.join(all_campaigns, on="campaign").select(
        "campaign_id", "channel", "day", *CAMPAIGN_METRICS
    ).write_parquet(OUT / "campaign_daily" / "part-0.parquet")
    return daily.height


def partition_inputs(files, columns, prefix, count, schema, cleaner):
    paths = normalize_csvs(files, columns, prefix, count)
    if count == 1:
        return cleaner(read_csv(paths[0], columns))
    for index, path in enumerate(paths):
        frame = cleaner(read_csv(path, columns))
        repartition(frame, prefix.with_name(f"{prefix.name}-{index:03}"), count)
    return pl.DataFrame(schema=schema)


def stage(label, started):
    peak_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print(f"{label} elapsed_seconds={time.perf_counter() - started:.3f} peak_rss_mib={peak_mib:.1f}")


def main():
    started = time.perf_counter()
    manifest = json.loads((DATA / "manifest.json").read_text(encoding="utf-8"))
    start_ms, end_ms = (
        int(datetime.fromisoformat(manifest[key].replace("Z", "+00:00")).timestamp() * 1000)
        for key in ("window_start", "window_end")
    )
    event_files = sorted((DATA / "events").glob("*.csv"))
    click_files = sorted((DATA / "clicks").glob("*.csv"))
    link_files = sorted((DATA / "links").glob("*.csv"))
    largest_input = max(
        sum(path.stat().st_size for path in event_files),
        sum(path.stat().st_size for path in click_files),
    )
    count = min(MAX_PARTITIONS, max(1, (largest_input + PARTITION_BYTES - 1) // PARTITION_BYTES))
    first_day = start_ms // DAY_MS
    days = (end_ms - 1) // DAY_MS - first_day + 1
    for name in ("user_daily", "campaign_daily"):
        (OUT / name).mkdir(parents=True, exist_ok=True)

    attribute = load_attribution()
    with tempfile.TemporaryDirectory(prefix="shopping-", dir=SCRATCH) as folder:
        scratch = Path(folder)
        products = read_input([DATA / "products.csv"], ["product_id", "category"], scratch / "products")
        campaigns = read_input([DATA / "campaigns.csv"], ["campaign_id", "channel"], scratch / "campaigns")
        campaign_ids = campaigns.select("campaign_id").with_row_index("campaign")
        user_map = linked_users(read_input(link_files, LINK_COLS, scratch / "links"))
        stage(f"linked users; {count} partitions", started)
        events = partition_inputs(
            event_files, EVENT_COLS, scratch / "events", count, EVENT_SCHEMA,
            lambda frame: clean_events(frame, user_map, start_ms, end_ms),
        )
        stage("cleaned events", started)
        clicks = partition_inputs(
            click_files, CLICK_COLS, scratch / "clicks", count, CLICK_SCHEMA,
            lambda frame: clean_clicks(frame, user_map, campaign_ids, start_ms, end_ms),
        )
        del user_map
        stage("cleaned clicks", started)

        totals = np.zeros((campaigns.height + 1, days, len(CAMPAIGN_METRICS)), dtype=np.int64)
        user_rows = 0
        for index in range(count):
            if count > 1:
                events = read_partition(scratch, "events", index, EVENT_SCHEMA)
                clicks = read_partition(scratch, "clicks", index, CLICK_SCHEMA)
            daily, sessions = summarize_users(events, products)
            if daily.height or index == 0:
                daily.write_parquet(OUT / "user_daily" / f"part-{index:03}.parquet")
            user_rows += daily.height
            credit_campaigns(attribute, sessions, clicks, first_day, totals)
        campaign_rows = write_campaigns(totals, first_day, campaigns)
    stage(f"wrote {user_rows} user-day rows and {campaign_rows} campaign-day rows", started)
    print(f"pipeline_seconds={time.perf_counter() - started:.6f}")


if __name__ == "__main__":
    main()
