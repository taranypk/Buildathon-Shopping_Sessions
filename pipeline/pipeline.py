#!/usr/bin/env python3
"""Exact daily shopping-session aggregation using Polars."""
import glob
import json
import os
import subprocess
import time
from datetime import datetime, timezone

NCPU = os.environ.get("NCPU")
if NCPU:
    os.environ.setdefault("POLARS_MAX_THREADS", NCPU)
import polars as pl

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
os.environ.setdefault("POLARS_TEMP_DIR", SCRATCH)

EVENT_COLS = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
CLICK_COLS = ["click_id", "user_id", "campaign_id", "ts"]
LINK_COLS = ["user_id_a", "user_id_b"]
SEVEN_DAYS = 604_800_000
THIRTY_MIN = 1_800_000
DAY_MS = 86_400_000


def read_csvs(files, columns):
    """Read a set of headered files with all values kept as strings."""
    if not files:
        return pl.DataFrame(schema={c: pl.String for c in columns})
    # The narrow C validator preserves LF-only records and removes malformed
    # rows before Polars' faster columnar parser sees the files.
    temp = os.path.join(SCRATCH, "csv_filter")
    os.makedirs(temp, exist_ok=True)
    path = os.path.join(temp, "_".join(columns) + ".csv")
    subprocess.run(
        ["/usr/local/bin/csv_filter", str(len(columns)), ",".join(columns), path, *files],
        check=True,
    )
    return pl.read_csv(
        path,
        schema_overrides={c: pl.String for c in columns},
        infer_schema_length=0,
        ignore_errors=True,
        truncate_ragged_lines=False,
        null_values="",
        quote_char='"',
        raise_if_empty=False,
    ).select(columns)


def epoch_ms_expr(name):
    s = pl.col(name)
    text = s.str.to_datetime(format="%+", time_unit="ms", time_zone="UTC", strict=False)
    digits = s.str.contains(r"^[0-9]+$")
    iso = s.str.contains(
        r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$"
    )
    return pl.when(digits).then(s.cast(pl.Int64, strict=False)).when(iso).then(text.cast(pl.Int64, strict=False)).otherwise(None)


def window_ms(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


class Components:
    """Disjoint set whose representative is always the byte-wise smallest ID."""
    def __init__(self):
        self.parent = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            return x
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while x != root:
            nxt = self.parent[x]
            self.parent[x] = root
            x = nxt
        return root

    def join(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            if ra < rb:
                self.parent[rb] = ra
            else:
                self.parent[ra] = rb

    def mapping_frame(self):
        ids = list(self.parent)
        return pl.DataFrame({"user_id": ids, "person": [self.find(x) for x in ids]}, schema={"user_id": pl.String, "person": pl.String})


def main():
    run_started = time.perf_counter()
    with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as f:
        manifest = json.load(f)
    start_ms = window_ms(manifest["window_start"])
    end_ms = window_ms(manifest["window_end"])

    products = read_csvs([os.path.join(DATA, "products.csv")], ["product_id", "category"])
    campaigns = read_csvs([os.path.join(DATA, "campaigns.csv")], ["campaign_id", "channel"])
    links = Components()
    for a, b in read_csvs(sorted(glob.glob(os.path.join(DATA, "links", "*.csv"))), LINK_COLS).iter_rows():
        if a and b:
            links.join(a, b)
    user_map = links.mapping_frame()

    events = read_csvs(sorted(glob.glob(os.path.join(DATA, "events", "*.csv"))), EVENT_COLS)
    if events.height:
        events = events.with_columns(
            epoch_ms_expr("ts").alias("event_ms"),
            epoch_ms_expr("ingest_ts").alias("ingest_ms"),
            pl.col("price_cents").cast(pl.Int64, strict=False).alias("price"),
            pl.col("price_cents").str.contains(r"^[0-9]+$").alias("price_digits"),
        )
        # Deduplication deliberately precedes all event validation.
        events = events.filter(pl.col("event_id").is_not_null() & (pl.col("event_id") != ""))
        events = events.sort(["event_id", "ingest_ms"]).unique(subset=["event_id"], keep="first", maintain_order=True)
        events = events.filter(
            pl.col("user_id").is_not_null() & (pl.col("user_id") != "")
            & pl.col("product_id").is_not_null() & (pl.col("product_id") != "")
            & pl.col("event_type").is_in(["view", "cart", "purchase"])
            & pl.col("event_ms").is_not_null() & (pl.col("event_ms") >= start_ms) & (pl.col("event_ms") < end_ms)
            & ((pl.col("ingest_ms") - pl.col("event_ms")) <= DAY_MS)
            & ((pl.col("event_type") != "purchase") | (pl.col("price_digits") & pl.col("price").is_between(1, 10_000_000)))
        )
        events = events.join(user_map, on="user_id", how="left").with_columns(pl.coalesce("person", "user_id").alias("person"))
        events = events.join(products, on="product_id", how="left").with_columns(pl.col("category").fill_null("unknown"))
        events = events.sort(["person", "event_ms"])
        events = events.with_columns(pl.col("event_ms").diff().over("person").alias("gap"))
        events = events.with_columns(
            (pl.col("gap").is_null() | (pl.col("gap") > THIRTY_MIN)).cast(pl.Int64).alias("new_session")
        ).with_columns(pl.col("new_session").cum_sum().over("person").alias("session_no"))
        events = events.with_columns(
            pl.col("event_ms").min().over(["person", "session_no"]).alias("session_start")
        )
        sessions = events.group_by(["person", "session_no", "session_start"]).agg(
            pl.len().cast(pl.Int64).alias("events"),
            pl.col("price").filter(pl.col("event_type") == "purchase").sum().fill_null(0).cast(pl.Int64).alias("revenue_cents"),
            (pl.col("event_type") == "purchase").any().alias("purchased"),
        ).with_columns(
            pl.from_epoch("session_start", time_unit="ms").dt.date().alias("day")
        )
        purchases = events.filter(pl.col("event_type") == "purchase").group_by(
            ["person", "session_no", "category"]
        ).agg(pl.col("price").sum().alias("category_revenue"))
        top = purchases.sort(["person", "session_no", "category_revenue", "category"], descending=[False, False, True, False]).unique(
            subset=["person", "session_no"], keep="first"
        ).select("person", "session_no", pl.col("category").alias("top_category"))
        sessions = sessions.join(top, on=["person", "session_no"], how="left")
    else:
        sessions = pl.DataFrame(schema={
            "person": pl.String, "session_no": pl.Int64, "session_start": pl.Int64,
            "events": pl.Int64, "revenue_cents": pl.Int64, "purchased": pl.Boolean,
            "day": pl.Date, "top_category": pl.String,
        })

    if sessions.height:
        # Category revenue must be summed across all sessions of the day.
        user_cat = events.filter(pl.col("event_type") == "purchase").with_columns(
            pl.from_epoch(pl.col("session_start"), time_unit="ms").dt.date().alias("day")
        ).group_by(["person", "day", "category"]).agg(pl.col("price").sum().alias("cat_rev"))
        user_top = user_cat.sort(["person", "day", "cat_rev", "category"], descending=[False, False, True, False]).unique(
            subset=["person", "day"], keep="first"
        ).select("person", "day", pl.col("category").alias("top_category"))
        user_daily = sessions.group_by(["person", "day"]).agg(
            pl.len().cast(pl.Int64).alias("sessions"), pl.col("events").sum().cast(pl.Int64),
            pl.col("revenue_cents").sum().cast(pl.Int64),
        ).join(user_top, on=["person", "day"], how="left").rename({"person": "user_id"})
    else:
        user_daily = pl.DataFrame(schema={"user_id": pl.String, "day": pl.Date, "sessions": pl.Int64, "events": pl.Int64, "revenue_cents": pl.Int64, "top_category": pl.String})

    clicks = read_csvs(sorted(glob.glob(os.path.join(DATA, "clicks", "*.csv"))), CLICK_COLS)
    if clicks.height:
        clicks = clicks.filter(pl.col("click_id").is_not_null() & (pl.col("click_id") != ""))
        clicks = clicks.unique(subset=["click_id"], keep="first")
        clicks = clicks.with_columns(epoch_ms_expr("ts").alias("click_ms"))
        clicks = clicks.filter(
            pl.col("user_id").is_not_null() & (pl.col("user_id") != "")
            & pl.col("click_ms").is_not_null() & (pl.col("click_ms") >= start_ms - SEVEN_DAYS) & (pl.col("click_ms") < end_ms)
        ).join(campaigns, on="campaign_id", how="inner")
        clicks = clicks.join(user_map, on="user_id", how="left").with_columns(
            pl.coalesce("person", "user_id").alias("person"), pl.col("click_ms").alias("click_time")
        )
    else:
        clicks = pl.DataFrame(schema={"click_id": pl.String, "campaign_id": pl.String, "channel": pl.String, "person": pl.String, "click_ms": pl.Int64, "click_time": pl.Int64})

    if sessions.height and clicks.height:
        best_click = clicks.sort(["person", "click_time", "click_id"]).unique(subset=["person", "click_time"], keep="first")
        credited = sessions.sort(["person", "session_start"]).join_asof(
            best_click.sort(["person", "click_time"]), left_on="session_start", right_on="click_time",
            by="person", strategy="backward", tolerance=SEVEN_DAYS,
        ).with_columns(pl.col("campaign_id").fill_null("organic"), pl.col("channel").fill_null("organic"))
        # Every distinct campaign in the seven-day lookback is an assist.
        assists = sessions.join_where(
            clicks,
            pl.col("person") == pl.col("person_right"),
            pl.col("click_ms") <= pl.col("session_start"),
            pl.col("click_ms") >= pl.col("session_start") - SEVEN_DAYS,
        ).select("person", "session_no", "day", "campaign_id").unique()
        assist_daily = assists.group_by(["campaign_id", "day"]).agg(pl.len().cast(pl.Int64).alias("assisted_sessions"))
    elif sessions.height:
        credited = sessions.with_columns(pl.lit("organic").alias("campaign_id"), pl.lit("organic").alias("channel"))
        assist_daily = pl.DataFrame(schema={"campaign_id": pl.String, "day": pl.Date, "assisted_sessions": pl.Int64})
    else:
        credited = sessions.with_columns(pl.lit("organic").alias("campaign_id"), pl.lit("organic").alias("channel"))
        assist_daily = pl.DataFrame(schema={"campaign_id": pl.String, "day": pl.Date, "assisted_sessions": pl.Int64})

    if credited.height:
        credited_daily = credited.group_by(["campaign_id", "channel", "day"]).agg(
            pl.len().cast(pl.Int64).alias("sessions"), pl.col("purchased").sum().cast(pl.Int64).alias("purchasing_sessions"),
            pl.col("revenue_cents").sum().cast(pl.Int64),
        )
        campaign_daily = credited_daily.join(assist_daily, on=["campaign_id", "day"], how="full", coalesce=True)
    else:
        campaign_daily = pl.DataFrame(schema={"campaign_id": pl.String, "channel": pl.String, "day": pl.Date, "sessions": pl.Int64, "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64, "assisted_sessions": pl.Int64})
    campaign_daily = campaign_daily.with_columns(
        pl.col("channel").fill_null(pl.col("campaign_id").replace(dict(campaigns.select("campaign_id", "channel").iter_rows())).fill_null("organic")),
        pl.col("sessions").fill_null(0).cast(pl.Int64), pl.col("purchasing_sessions").fill_null(0).cast(pl.Int64),
        pl.col("revenue_cents").fill_null(0).cast(pl.Int64), pl.col("assisted_sessions").fill_null(0).cast(pl.Int64),
    ) if campaign_daily.height else campaign_daily

    os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
    user_daily.select("user_id", "day", "sessions", "events", "revenue_cents", "top_category").write_parquet(os.path.join(OUT, "user_daily", "part-0.parquet"))
    campaign_daily.select("campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions").write_parquet(os.path.join(OUT, "campaign_daily", "part-0.parquet"))
    print(f"wrote {user_daily.height} user-day rows and {campaign_daily.height} campaign-day rows")
    print(f"pipeline_seconds={time.perf_counter() - run_started:.6f}")


if __name__ == "__main__":
    main()
