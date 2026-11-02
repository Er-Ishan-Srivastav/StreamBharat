"""Q3 — Parse AkashCDN and NimbusEdge logs into a common schema with quarantine."""
from __future__ import annotations
import argparse, os
from pyspark.sql import functions as F
from .spark_session import create_spark

AKASH_RE=r'^(\S+)\s+(\S+)\s+(\S+)\s+"GET /v/([^/]+)/([^/]+)/([^/.]+)\.ts HTTP/[^"]+"\s+(\d{3})\s+(\d+)\s+"([^"]+)"$'
NIMBUS_RE=r'^(\d+)\t([^\t]+)\t([^\t]+)\tvod/([^/]+)/([^/]+)/([^\t]+)\t(\d{3})\t(\d+)\t(.+)$'
COLS=["ts","cdn","edge","client_hash","content_id","quality","segment","status","bytes","user_agent"]

def run_q3(akash_path="data/raw/cdn/akashcdn_2026-09.log", nimbus_path="data/raw/cdn/nimbusedge_2026-09.tsv",
           edge_map_path="data/raw/reference/cdn_edges.csv", bronze="data/delta/bronze/cdn_requests",
           silver="data/delta/silver/cdn_usage_hourly"):
    spark=create_spark("StreamBharat-Q3-CDN-Parsing")
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        a=spark.read.text(akash_path).withColumn("source_file",F.input_file_name()).withColumn("line",F.col("value"))
        for i,name in enumerate(COLS):
            idx={"ts":1,"edge":2,"client_hash":3,"content_id":4,"quality":5,"segment":6,"status":7,"bytes":8,"user_agent":9}.get(name,1)
            if name=="ts": a=a.withColumn(name,F.to_timestamp(F.regexp_extract("line",AKASH_RE,idx)))
            elif name=="cdn": a=a.withColumn(name,F.lit("AkashCDN"))
            else: a=a.withColumn(name,F.regexp_extract("line",AKASH_RE,idx))
        a=a.withColumn("bytes",F.col("bytes").cast("long")).withColumn("status",F.col("status").cast("int"))
        n=spark.read.text(nimbus_path).withColumn("source_file",F.input_file_name()).withColumn("line",F.col("value"))
        n=n.withColumn("epoch",F.regexp_extract("line",NIMBUS_RE,1).cast("long"))
        n=n.withColumn("ts",F.to_timestamp(F.from_unixtime("epoch"))).withColumn("cdn",F.lit("NimbusEdge"))
        for name,idx in [("edge",2),("client_hash",3),("content_id",4),("quality",5),("segment",6),("status",7),("bytes",8),("user_agent",9)]:
            n=n.withColumn(name,F.regexp_extract("line",NIMBUS_RE,idx))
        n=n.withColumn("status",F.col("status").cast("int")).withColumn("bytes",F.col("bytes").cast("long"))
        # Provider-specific edge identifiers are joined through edge / pop_code.
        edges=spark.read.option("header",True).csv(edge_map_path)
        a=a.join(edges.select(F.col("edge").alias("edge_key"),F.col("region").alias("region")),a.edge==F.col("edge_key"),"left").drop("edge_key")
        n=n.join(edges.select(F.col("pop_code").alias("edge_key"),F.col("region").alias("region")),n.edge==F.col("edge_key"),"left").drop("edge_key")
        parsed=a.select(*COLS,"region","source_file","line").unionByName(n.select(*COLS,"region","source_file","line"))
        valid=parsed.filter(F.col("ts").isNotNull()&F.col("content_id").isNotNull()&(F.col("content_id")!="")&F.col("bytes").isNotNull())
        bad=parsed.filter(~(F.col("ts").isNotNull()&F.col("content_id").isNotNull()&(F.col("content_id")!="")&F.col("bytes").isNotNull()))
        valid.write.format("delta").mode("overwrite").save(bronze)
        bad.write.format("delta").mode("overwrite").save("data/delta/quarantine/cdn_bad_lines")
        counts=bad.groupBy("source_file").count()
        counts.write.format("delta").mode("overwrite").save("data/delta/quality/cdn_quarantine_counts")
        hourly=(valid.filter(F.col("status")==200).withColumn("hour",F.date_trunc("hour","ts"))
            .groupBy("hour","cdn","region","content_id").agg(F.sum("bytes").alias("bytes"),
                (F.sum("bytes")/F.lit(1024**3)).alias("gb"),F.count("*").alias("requests")))
        hourly.write.format("delta").mode("overwrite").save(silver)
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--akash",default="data/raw/cdn/akashcdn_2026-09.log"); p.add_argument("--nimbus",default="data/raw/cdn/nimbusedge_2026-09.tsv"); a=p.parse_args(); run_q3(a.akash,a.nimbus)
