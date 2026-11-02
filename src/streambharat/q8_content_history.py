"""Q8 — SCD Type 2 catalogue history and point-in-time regional licence checks."""
from __future__ import annotations
import argparse, glob, os
from pyspark.sql import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from .q1_catalog import read_catalog_snapshot
from .spark_session import create_spark


def run_q8(catalog_dir="data/raw/content", playback_path="data/delta/silver/playback_clean",
           city_state_path="data/raw/reference/city_state.csv", licences_path="data/raw/content/licences.csv",
           scd_path="data/delta/gold/dim_content_scd2", violations_path="data/delta/gold/licence_violations"):
    spark=create_spark("StreamBharat-Q8-Content-SCD2")
    try:
        files=sorted(glob.glob(os.path.join(catalog_dir,"catalog_*.json")))
        if not files: raise FileNotFoundError(f"No monthly catalogue snapshots under {catalog_dir}")
        snapshots=None
        history_path="data/delta/reference/catalog_snapshot_history"
        aug1_version=None
        for index,file in enumerate(files):
            frame=read_catalog_snapshot(spark,file).dropDuplicates(["content_id","snapshot_date"])
            snapshots=frame if snapshots is None else snapshots.unionByName(frame)
            # Each monthly source snapshot is a separate Delta commit. This allows
            # a reproducible versionAsOf read for the 1-Aug-2026 catalogue state.
            frame.write.format("delta").mode("overwrite" if index==0 else "append").save(history_path)
            if os.path.basename(file).endswith("2026-08-01.json"):
                aug1_version=int(DeltaTable.forPath(spark,history_path).history(1).select("version").first()["version"])
        # A change to any tracked business attribute closes the previous version.
        tracked=["title","tier","partner_id","age_rating","duration_sec"]
        digest=F.sha2(F.concat_ws("||",*[F.coalesce(F.col(c).cast("string"),F.lit("<NULL>")) for c in tracked]),256)
        w=Window.partitionBy("content_id").orderBy("snapshot_date")
        versions=(snapshots.withColumn("row_hash",digest).withColumn("prev_hash",F.lag("row_hash").over(w))
            .filter(F.col("prev_hash").isNull()|(F.col("row_hash")!=F.col("prev_hash")))
            .withColumn("valid_from",F.col("snapshot_date"))
            .withColumn("valid_to",F.lead("valid_from").over(Window.partitionBy("content_id").orderBy("valid_from")))
            .withColumn("is_current",F.col("valid_to").isNull()))
        versions.write.format("delta").mode("overwrite").save(scd_path)
        plays=(spark.read.format("delta").load(playback_path)
            .filter(F.col("event_type").isin("start","heartbeat","complete"))
            .withColumn("view_ts",F.to_timestamp("event_ts_corrected"))
            .withColumn("view_date",F.to_date("view_ts")))
        city_state=spark.read.option("header",True).csv(city_state_path)
        licences=(spark.read.option("header",True).option("inferSchema",True).csv(licences_path)
            .withColumn("licence_start",F.to_date("licence_start")).withColumn("licence_end",F.to_date("licence_end")))
        regional=(plays.join(city_state,"city","left")
            .join(licences,(plays.content_id==licences.content_id)&(city_state.state==licences.region)&
                  plays.view_date.between(licences.licence_start,licences.licence_end),"left")
            .withColumn("licence_valid",F.col("licence_start").isNotNull())
            .filter(~F.col("licence_valid")))
        regional.write.format("delta").mode("overwrite").save(violations_path)
        # Delta time travel to the commit that loaded the 1-Aug snapshot, then retain
        # only that snapshot's rows (the commit contains earlier snapshots too).
        if aug1_version is None:
            raise ValueError("The 2026-08-01 catalogue snapshot is required for the Q8 time-travel task")
        as_of_snapshot=(spark.read.format("delta").option("versionAsOf",aug1_version).load(history_path)
            .filter(F.col("snapshot_date")==F.lit("2026-08-01").cast("date")))
        as_of_snapshot.write.format("delta").mode("overwrite").save("data/delta/reference/catalog_as_of_2026_08_01")
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--catalog-dir",default="data/raw/content"); a=p.parse_args(); run_q8(catalog_dir=a.catalog_dir)
