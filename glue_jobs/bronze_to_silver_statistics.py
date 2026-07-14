"""
Glue Job: Bronze → Silver (Statistics Data) - FIXED VERSION
"""

import sys
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)
from pyspark.sql.window import Window

# ── Job Setup ────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_database",
    "bronze_table",
    "silver_bucket",
    "silver_database",
    "silver_table",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# ── Config ───────────────────────────────────────────────────────────────────
BRONZE_DB = args["bronze_database"]
BRONZE_TABLE = args["bronze_table"]  # Not used directly anymore
SILVER_BUCKET = args["silver_bucket"]
SILVER_DB = args["silver_database"]
SILVER_TABLE = args["silver_table"]
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/statistics/"

logger.info(f"Bronze DB: {BRONZE_DB}")
logger.info(f"Silver: {SILVER_DB}.{SILVER_TABLE} → {SILVER_PATH}")


# ── Helper: Normalize CSV Schema ──────────────────────────────────────────
def normalize_csv_schema(df):
    """Apply schema enforcement to Kaggle CSV format."""
    return df.select(
        F.col("video_id").cast(StringType()).alias("video_id"),
        F.col("trending_date").cast(StringType()).alias("trending_date"),
        F.col("title").cast(StringType()).alias("title"),
        F.col("channel_title").cast(StringType()).alias("channel_title"),
        F.col("category_id").cast(LongType()).alias("category_id"),
        F.col("publish_time").cast(StringType()).alias("publish_time"),
        F.col("tags").cast(StringType()).alias("tags"),
        F.col("views").cast(LongType()).alias("views"),
        F.col("likes").cast(LongType()).alias("likes"),
        F.col("dislikes").cast(LongType()).alias("dislikes"),
        F.col("comment_count").cast(LongType()).alias("comment_count"),
        F.col("thumbnail_link").cast(StringType()).alias("thumbnail_link"),
        F.col("comments_disabled").cast(BooleanType()).alias("comments_disabled"),
        F.col("ratings_disabled").cast(BooleanType()).alias("ratings_disabled"),
        F.col("video_error_or_removed").cast(BooleanType()).alias("video_error_or_removed"),
        F.col("description").cast(StringType()).alias("description"),
        F.col("region").cast(StringType()).alias("region"),
    )


# ── Helper: Normalize JSON Schema ─────────────────────────────────────────
def normalize_json_schema(df):
    """Apply schema enforcement to YouTube API JSON format."""
    columns = set(df.columns)
    
    return df.select(
        F.col("id").alias("video_id"),
        F.lit(datetime.utcnow().strftime("%y.%d.%m")).alias("trending_date"),
        F.col("`snippet.title`").alias("title") if "snippet.title" in columns
            else F.col("snippet__title").alias("title"),
        F.col("`snippet.channelTitle`").alias("channel_title") if "snippet.channelTitle" in columns
            else F.col("snippet__channelTitle").alias("channel_title"),
        F.col("`snippet.categoryId`").cast(LongType()).alias("category_id") if "snippet.categoryId" in columns
            else F.col("snippet__categoryId").cast(LongType()).alias("category_id"),
        F.col("`snippet.publishedAt`").alias("publish_time") if "snippet.publishedAt" in columns
            else F.col("snippet__publishedAt").alias("publish_time"),
        F.col("`snippet.tags`").alias("tags") if "snippet.tags" in columns
            else F.lit(None).cast(StringType()).alias("tags"),
        F.col("`statistics.viewCount`").cast(LongType()).alias("views") if "statistics.viewCount" in columns
            else F.col("statistics__viewCount").cast(LongType()).alias("views"),
        F.col("`statistics.likeCount`").cast(LongType()).alias("likes") if "statistics.likeCount" in columns
            else F.col("statistics__likeCount").cast(LongType()).alias("likes"),
        F.col("`statistics.dislikeCount`").cast(LongType()).alias("dislikes") if "statistics.dislikeCount" in columns
            else F.lit(0).cast(LongType()).alias("dislikes"),
        F.col("`statistics.commentCount`").cast(LongType()).alias("comment_count") if "statistics.commentCount" in columns
            else F.col("statistics__commentCount").cast(LongType()).alias("comment_count"),
        F.col("`snippet.thumbnails.default.url`").alias("thumbnail_link") if "snippet.thumbnails.default.url" in columns
            else F.lit(None).cast(StringType()).alias("thumbnail_link"),
        F.lit(False).alias("comments_disabled"),
        F.lit(False).alias("ratings_disabled"),
        F.lit(False).alias("video_error_or_removed"),
        F.col("`snippet.description`").alias("description") if "snippet.description" in columns
            else F.col("snippet__description").alias("description"),
        F.col("region").alias("region"),
    )


# ── Step 1: Read from Bronze (CSV + JSON) ──────────────────────────────────
logger.info("Reading from Bronze catalog (CSV + JSON)...")

predicate = "region in ('ca','gb','us','in')"

# Read and normalize CSV table (Kaggle historical data)
logger.info("Reading CSV table: statistics_csv")
csv_datasource = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name="statistics_csv",
    transformation_ctx="csv_datasource",
    #push_down_predicate=predicate,
)
csv_df = csv_datasource.toDF()
csv_count = csv_df.count()
logger.info(f"DEBUG: CSV table schema: {csv_df.schema}")
logger.info(f"DEBUG: CSV sample row: {csv_df.limit(1).collect()}")
logger.info(f"CSV records: {csv_count}")
if csv_count > 0:
    csv_df = normalize_csv_schema(csv_df)
    logger.info(f"CSV records normalized: {csv_count}")

# Read and normalize JSON table (YouTube API streaming data)
logger.info("Reading JSON table: statistics_json")
json_datasource = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name="statistics_json",
    transformation_ctx="json_datasource",
   # push_down_predicate=predicate,
)
json_df = json_datasource.toDF()
json_count = json_df.count()
logger.info(f"DEBUG: JSON table schema: {json_df.schema}")
logger.info(f"DEBUG: JSON sample row: {json_df.limit(1).collect()}")
logger.info(f"JSON records: {json_count}")
if json_count > 0:
    json_df = normalize_json_schema(json_df)
    logger.info(f"JSON records normalized: {json_count}")

# Union both sources (now both have identical schema)
if csv_count > 0 and json_count > 0:
    combined_df = csv_df.unionByName(json_df)
elif csv_count > 0:
    combined_df = csv_df
elif json_count > 0:
    combined_df = json_df
else:
    combined_df = csv_df  # Empty DataFrame with schema

df = combined_df
initial_count = df.count()
logger.info(f"Bronze records read (combined): {initial_count} (CSV: {csv_count}, JSON: {json_count})")

if initial_count == 0:
    logger.info("No new records to process. Committing empty job.")
else:
    # ── Step 3: Data Cleansing ──────────────────────────────────────────────
    logger.info("Cleansing data...")

    # Remove records where video_id is null (corrupt rows)
    df = df.filter(F.col("video_id").isNotNull())

    # Standardize region codes to lower
    df = df.withColumn("region", F.lower(F.trim(F.col("region"))))

    # Parse trending_date from Kaggle format (YY.DD.MM) to proper date
    df = df.withColumn(
        "trending_date_parsed",
        F.when(
            F.col("trending_date").rlike(r"^\d{2}\.\d{2}\.\d{2}$"),
            F.to_date(F.col("trending_date"), "yy.dd.MM")
        ).otherwise(
            F.to_date(F.col("trending_date"))
        )
    )

    # Fill nulls for numeric columns with 0
    numeric_cols = ["views", "likes", "dislikes", "comment_count"]
    for col_name in numeric_cols:
        df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

    # Add derived columns
    df = df.withColumn("like_ratio",
        F.when(
            (F.col("views") > 0),
            F.round(F.col("likes") / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )
    df = df.withColumn("engagement_rate",
        F.when(
            (F.col("views") > 0),
            F.round((F.col("likes") + F.col("dislikes") + F.col("comment_count")) / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )

    # Add processing metadata
    df = df.withColumn("_processed_at", F.current_timestamp())
    df = df.withColumn("_job_name", F.lit(args["JOB_NAME"]))


    # ── Step 4: Deduplication ───────────────────────────────────────────────
    logger.info("Deduplicating...")

    window = Window.partitionBy("video_id", "region", "trending_date_parsed") \
        .orderBy(F.col("_processed_at").desc())

    df = df.withColumn("_row_num", F.row_number().over(window)) \
        .filter(F.col("_row_num") == 1) \
        .drop("_row_num")

    clean_count = df.count()
    logger.info(f"After cleansing & dedup: {clean_count} records (removed {initial_count - clean_count})")


    # ── Step 5: Data Quality Checks ─────────────────────────────────────────
    logger.info("Running data quality checks...")

    null_counts = {}
    for col_name in ["video_id", "title", "channel_title", "views"]:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_counts[col_name] = null_count
        if null_count > 0:
            logger.warning(f"  DQ WARNING: {col_name} has {null_count} null values")

    negative_views = df.filter(F.col("views") < 0).count()
    if negative_views > 0:
        logger.warning(f"  DQ WARNING: {negative_views} records with negative views")

    logger.info(f"  DQ check complete. Null counts: {null_counts}")


    # ── Step 6: Write to Silver Layer ───────────────────────────────────────
    logger.info(f"Writing to Silver: {SILVER_PATH}")

    # Convert back to DynamicFrame for Glue-native write
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "silver_statistics")

    sink = glueContext.getSink(
        connection_type="s3",
        path=SILVER_PATH,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["region"],
    )
    sink.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName=SILVER_TABLE)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dynamic_frame)

    logger.info(f"Silver write complete. {clean_count} records written.")

job.commit()