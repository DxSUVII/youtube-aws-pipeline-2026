# YouTube Trending Data Pipeline

> A cloud-native, production-grade ETL pipeline that ingests live YouTube trending video data across 4 regions, transforms it through a **Medallion Architecture** (Bronze → Silver → Gold), enforces data quality gates, and produces analytics-ready aggregations — all orchestrated by AWS Step Functions.

---

## Architecture Diagram

![YouTube Trending Data Pipeline Architecture](YouTube%20Trending%20Data%20Pipeline.png)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
  - [Bronze Layer](#bronze-layer-raw-data)
  - [Silver Layer](#silver-layer-cleansed-data)
  - [Data Quality Gate](#data-quality-gate)
  - [Gold Layer](#gold-layer-business-aggregations)
- [Pipeline Orchestration](#pipeline-orchestration)
- [Setup & Deployment](#setup--deployment)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring & Alerting](#monitoring--alerting)
- [Production Issues & Debugging Journal](#production-issues--debugging-journal)
- [Key Learnings](#key-learnings)

---

## Overview

This pipeline automates end-to-end collection, cleaning, and analysis of YouTube trending video data. It replaces manual Kaggle CSV downloads with a live **YouTube Data API v3** integration and produces three analytics tables in the Gold layer:

- **Trending Analytics** — daily trending metrics per region (views, engagement rates, unique channels)
- **Channel Analytics** — channel-level performance and regional rankings
- **Category Analytics** — category breakdowns with view share percentages

The pipeline runs on a configurable schedule via **Amazon EventBridge** and handles both historical (Kaggle CSV) and live (API JSON) data sources in parallel.

---

## Architecture

```
Data Sources        Bronze (S3)          Silver (S3)         Quality Gate        Gold (S3)           Analytics
┌──────────┐     ┌─────────────┐     ┌─────────────┐     ┌────────────┐     ┌─────────────┐     ┌──────────┐
│ YouTube  │     │             │     │             │     │            │     │ trending_   │     │          │
│ API v3   │────▶│  Raw JSON   │────▶│  Cleansed   │────▶│ DQ Lambda  │────▶│ analytics   │────▶│  Athena  │
│          │     │  (S3)       │     │  Parquet    │     │ Validates  │     │             │     │          │
├──────────┤     │             │     │  (S3)       │     │ row count  │     │ channel_    │     ├──────────┤
│ Kaggle   │     │  Raw CSV    │     │             │     │ nulls      │     │ analytics   │     │ Quick-   │
│ Dataset  │────▶│  (S3)       │     │  Reference  │     │ schema     │     │             │     │ Sight    │
└──────────┘     │             │     │  Parquet    │     │ freshness  │     │ category_   │     └──────────┘
                 └─────────────┘     └─────────────┘     └─────┬──────┘     │ analytics   │
                                                               │            └─────────────┘
                                                          fail │
                                                               ▼
                                                         ┌────────────┐
                                                         │ SNS Alert  │
                                                         └────────────┘

Step Functions Orchestration:
Ingestion ──▶ Wait ──▶ Silver transforms (parallel) ──▶ Data Quality ──▶ Gold aggregation ──▶ SNS notification
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Compute | AWS Lambda, AWS Glue (PySpark) |
| Storage | Amazon S3 (Parquet, Snappy compression) |
| Orchestration | AWS Step Functions |
| Scheduling | Amazon EventBridge |
| Metadata Catalog | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| Alerting | Amazon SNS |
| Monitoring | Amazon CloudWatch |
| Security | AWS IAM |
| Languages | Python 3.11, PySpark, SQL |
| Libraries | Pandas, AWS Wrangler, Boto3 |
| Data Formats | Raw: JSON, CSV → Processed: Parquet (Snappy) |

---

## Project Structure

```
youtube-data-pipeline/
│
├── lambdas/
│   ├── youtube_api_ingestion/
│   │   └── lambda_function.py        # Fetches trending videos & categories from YouTube API
│   └── json_to_parquet/
│       └── lambda_function.py        # Converts JSON category mappings → Parquet (Silver)
│
├── glue_jobs/
│   ├── bronze_to_silver_statistics.py  # PySpark: raw JSON/CSV → cleansed Parquet
│   └── silver_to_gold_analytics.py     # PySpark: cleansed Parquet → business aggregations
│
├── data_quality/
│   └── dq_lambda.py                  # Data quality validation gate
│
├── step_functions/
│   └── pipeline_orchestration.json   # Step Functions state machine definition
│
├── iam_permission/
│   ├── lambda_policy.json            # IAM policy for Lambda execution role
│   ├── glue_policy.json              # IAM policy for Glue execution role
│   └── stepfunctions_policy.json     # IAM policy for Step Functions role
│
├── scripts/
│   ├── aws_copy.sh                   # Upload historical Kaggle data to Bronze S3
│   └── information.md                # AWS resource names & configuration reference
│
├── data/
│   ├── {region}videos.csv            # Kaggle trending video datasets
│   └── {region}_category_id.json     # YouTube category ID mappings
│
└── YouTube Trending Data Pipeline.png
```

---

## Data Flow

### Bronze Layer (Raw Data)

The ingestion Lambda (`youtube_api_ingestion`) runs on an EventBridge schedule and fetches two things per region from the YouTube Data API v3:

**Trending Videos** (statistics data) — top 50 trending videos with snippet, statistics, and content details.

**Category Mappings** (reference data) — video category ID-to-name lookup table (e.g. `10 = Music`, `23 = Comedy`).

S3 paths written:
```
s3://bronze-bucket/youtube/raw_statistics/region=us/date=2026-07-14/hour=10/{id}.json
s3://bronze-bucket/youtube/raw_statistics_reference_data/region=us/date=2026-07-14/{id}_category_id.json
```

Historical Kaggle CSV data is also loaded into Bronze via `aws_copy.sh`:
```
s3://bronze-bucket/youtube/raw_statistics/region=us/USvideos.csv
```

> **Note:** The pipeline handles both formats in parallel — live API JSON and historical CSV — since they have different schemas that must be normalized in Silver.

---

### Silver Layer (Cleansed Data)

Two transformations run **in parallel** via Step Functions:

#### 1. Statistics — Glue PySpark Job (`bronze_to_silver_statistics`)

- Reads both CSV (Kaggle) and JSON (API) from Bronze
- Normalizes schemas across both formats into a single unified schema
- Type casts: `views`, `likes`, `dislikes`, `comment_count` → `Long`
- Parses `trending_date` across two formats (`yy.dd.MM` from Kaggle, ISO from API)
- Derives metrics: `like_ratio`, `engagement_rate`
- Deduplicates (latest record per `video_id / region / trending_date`)
- Writes partitioned Parquet with Snappy compression

Output: `s3://silver-bucket/youtube/statistics/region=us/`

Verified via Athena — row counts per region:

| Region | Rows |
|---|---|
| US | ~41,000 |
| CA | ~41,000 |
| GB | ~38,800 |
| IN | ~32,500 |

#### 2. Reference Data — Lambda (`json_to_parquet`)

- Reads category JSON from Bronze
- Flattens nested structure using `pd.json_normalize(raw["items"])`
- Deduplicates by `id`
- Registers Parquet in Glue Catalog

Output: `s3://silver-bucket/youtube/reference_data/region=US/`

---

### Data Quality Gate

Before data moves to Gold, the DQ Lambda validates Silver data against these checks:

| Check | Threshold |
|---|---|
| Row count | ≥ 10 rows |
| Null percentage | ≤ 5% on critical columns |
| Schema validation | Required columns present |
| Value range | Views sanity check (> 0) |
| Data freshness | < 48 hours since last ingestion |

If any check fails → pipeline halts → SNS alert sent → Gold aggregation **does not run**.

---

### Gold Layer (Business Aggregations)

The Glue job (`silver_to_gold_analytics`) produces three analytics tables:

#### `trending_analytics`
Daily trending metrics aggregated per region.

| Column | Description |
|---|---|
| region | Country code |
| trending_date_parsed | Date of snapshot |
| total_videos | Number of trending videos |
| total_views | Sum of all views |
| total_likes | Sum of all likes |
| avg_views_per_video | Average views per video |
| avg_like_ratio | Average like-to-view ratio |
| avg_engagement_rate | Average engagement rate |
| unique_channels | Count of distinct channels |
| unique_categories | Count of distinct categories |

#### `channel_analytics`
Channel-level performance and regional rankings.

| Column | Description |
|---|---|
| channel_title | YouTube channel name |
| region | Country code |
| total_videos | Videos that trended |
| total_views | Total views across trending videos |
| avg_engagement_rate | Average engagement rate |
| times_trending | Number of trending appearances |
| rank_in_region | Performance rank within region |
| categories | Categories the channel appears in |

#### `category_analytics`
Category-level breakdowns with view share.

| Column | Description |
|---|---|
| category | Video category name |
| region | Country code |
| trending_date_parsed | Date of snapshot |
| video_count | Videos in category |
| total_views | Total views for category |
| avg_engagement_rate | Average engagement rate |
| view_share_pct | Percentage of total regional views |

All Gold tables: Parquet (Snappy), partitioned by region, registered in Glue Data Catalog.

---

## Pipeline Orchestration

AWS Step Functions coordinates the full pipeline with retry logic (3 attempts, exponential backoff) and parallel execution:

```
1. IngestFromYouTubeAPI      → Lambda fetches 4 regions → Bronze S3
2. WaitForS3Consistency      → 10-second wait (S3 eventual consistency)
3. ProcessInParallel         → Two branches run simultaneously:
   ├── TransformReferenceData  → Lambda: JSON → Parquet (Silver)
   └── RunBronzeToSilverGlueJob → Glue: CSV/JSON → Parquet (Silver)
4. RunDataQualityChecks      → Lambda validates Silver data
5. EvaluateDataQuality       → Choice: pass → Gold, fail → SNS alert
6. RunSilverToGoldGlueJob    → Glue: Silver → Gold aggregations
7. NotifySuccess             → SNS email notification
```

Each stage has its own failure notification (NotifyIngestionFailure, NotifyTransformFailure, etc.).

---

## Setup & Deployment

### Prerequisites

- AWS account with permissions for Lambda, Glue, S3, Step Functions, SNS, IAM, Athena, EventBridge
- YouTube Data API v3 key from [Google Cloud Console](https://console.cloud.google.com/)
- AWS CLI configured

### 1. Create S3 Buckets

```bash
aws s3 mb s3://yt-data-pipeline-bronze-<region>-<env>
aws s3 mb s3://yt-data-pipeline-silver-<region>-<env>
aws s3 mb s3://yt-data-pipeline-gold-<region>-<env>
aws s3 mb s3://yt-data-pipeline-scripts-<region>-<env>
```

### 2. Create Glue Databases

```bash
aws glue create-database --database-input '{"Name": "yt_pipeline_bronze_dev"}'
aws glue create-database --database-input '{"Name": "yt_pipeline_silver_dev"}'
aws glue create-database --database-input '{"Name": "yt_pipeline_gold_dev"}'
```

### 3. Create SNS Topic

```bash
aws sns create-topic --name yt-data-pipeline-alerts
aws sns subscribe --topic-arn <topic-arn> --protocol email --notification-endpoint <your-email>
```

### 4. Upload Glue Scripts

```bash
aws s3 cp glue_jobs/bronze_to_silver_statistics.py s3://yt-data-pipeline-scripts-<region>-<env>/scripts/
aws s3 cp glue_jobs/silver_to_gold_analytics.py s3://yt-data-pipeline-scripts-<region>-<env>/scripts/
```

### 5. Deploy Lambda Functions

```bash
# Ingestion Lambda
cd lambdas/youtube_api_ingestion
zip function.zip lambda_function.py
aws lambda create-function \
  --function-name yt-data-pipeline-ingestion-dev \
  --runtime python3.11 \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://function.zip \
  --role <lambda-role-arn> \
  --timeout 300 \
  --memory-size 512 \
  --environment "Variables={
    YOUTUBE_API_KEY=<your-api-key>,
    S3_BUCKET_BRONZE=<bronze-bucket>,
    YOUTUBE_REGIONS=US,GB,CA,IN,
    SNS_ALERT_TOPIC_ARN=<sns-arn>
  }"
```

> Repeat for `json_to_parquet` and `data_quality` Lambdas.

### 6. Create Glue Jobs

```bash
aws glue create-job \
  --name yt-data-pipeline-bronze-to-silver \
  --role <glue-role-arn> \
  --command '{"Name":"glueetl","ScriptLocation":"s3://<scripts-bucket>/scripts/bronze_to_silver_statistics.py"}' \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X
```

### 7. Deploy Step Functions State Machine

```bash
aws stepfunctions create-state-machine \
  --name yt-data-pipeline-orchestration \
  --definition file://step_functions/pipeline_orchestration.json \
  --role-arn <stepfunctions-role-arn>
```

### 8. (Optional) Upload Historical Kaggle Data

```bash
cd data && bash ../scripts/aws_copy.sh
```

---

## Running the Pipeline

### Manual Trigger

```bash
aws stepfunctions start-execution \
  --state-machine-arn <state-machine-arn>
```

### Automated (EventBridge Schedule)

```bash
aws events put-rule \
  --name yt-pipeline-schedule \
  --schedule-expression "rate(6 hours)"

aws events put-targets \
  --rule yt-pipeline-schedule \
  --targets '[{"Id":"1","Arn":"<state-machine-arn>","RoleArn":"<eventbridge-role-arn>"}]'
```

### Sample Athena Queries

```sql
-- Top trending channels in the US
SELECT channel_title, total_views, times_trending
FROM yt_pipeline_gold_dev.channel_analytics
WHERE region = 'us'
ORDER BY total_views DESC
LIMIT 10;

-- Row counts per region in Silver
SELECT region, COUNT(*) as cnt
FROM yt_pipeline_silver_dev.clean_statistics
GROUP BY region;

-- Top categories by view share in Canada
SELECT category, view_share_pct, total_views
FROM yt_pipeline_gold_dev.category_analytics
WHERE region = 'ca'
ORDER BY view_share_pct DESC;
```

---

## Monitoring & Alerting

- **Step Functions Console** — visual execution graph, per-step status and error details
- **CloudWatch Logs** — detailed logs for all Lambda functions and Glue jobs
- **SNS Email Notifications** — alerts on success and failure at every pipeline stage
- **Amazon Athena** — query Silver and Gold tables directly to validate data

---

## Production Issues & Debugging Journal

This section documents every real production bug encountered during development. These were not textbook errors — they were discovered through actual pipeline execution and CloudWatch log analysis.

---

### Bug 1 — Glue Logger Incompatibility

**Error:**
```
Py4JError: An error occurred while calling o153.warning
```

**Root Cause:**
AWS Glue's logger is a Py4J wrapper, not a standard Python logger. It does not support `.warning()`.

**Fix:**
```python
# Before
logger.warning(f"Skipped region {region}: {e}")

# After
logger.info(f"Skipped region {region}: {e}")
```

**Lesson:** Always check service-specific SDK limitations. AWS managed services don't always expose the full Python standard library API.

---

### Bug 2 — Glue Catalog Partition Columns Not Registered

**Error:**
```
pushdown predicate: region in ('ca','gb','us','in')
can not be resolved against partition columns: []
```

**Root Cause:**
The Glue Catalog table `statistics_csv` was created without partition column metadata. Glue couldn't filter by `region` because it didn't know `region` was a partition key.

**Fix:**
Switched from Glue DynamicFrame with catalog read to direct Spark read using explicit S3 paths, bypassing the catalog partition issue entirely.

---

### Bug 3 — Multi-Line JSON Parsing Failure in PySpark

**Error:**
```
AnalysisException: column `id` cannot be resolved.
Did you mean `_corrupt_record`?
```

**Root Cause:**
The YouTube API returns pretty-printed, multi-line JSON with all video data nested inside an `items` array:
```json
{
  "kind": "youtube#videoListResponse",
  "items": [
    { "id": "abc123", "snippet": { ... }, "statistics": { ... } }
  ]
}
```
Spark's default JSON reader expects **newline-delimited JSON (NDJSON)** — one object per line. Reading a multi-line file as NDJSON caused the entire file to be dumped into `_corrupt_record`.

**Fix:**
```python
# Before — fails on multi-line JSON
json_df = spark.read.format("json").load(json_path)

# After — reads full file as single document, then explodes items array
raw_json_df = spark.read.format("json") \
    .option("multiLine", "true") \
    .load(json_path)

json_df = raw_json_df.select(F.explode("items").alias("item")).select("item.*")

# Re-attach region from file path since it's not a column inside the JSON
json_df = json_df.withColumn("_input_file", F.input_file_name())
json_df = json_df.withColumn("region",
    F.regexp_extract(F.col("_input_file"), r"region=([a-zA-Z]+)/", 1))
```

**Lesson:** Data format matters as much as data content. Always inspect raw files before writing Spark read logic.

---

### Bug 4 — IAM Missing `s3:ListBucket` on Silver Bucket

**Error:**
```
AccessDenied: not authorized to perform s3:ListBucket
on resource: arn:aws:s3:::yt-pipeline-silver-dev
```

**Root Cause:**
The Lambda role had `s3:PutObject` on `bucket/*` but was missing `s3:ListBucket` on `bucket` (without the `/*`). AWS Wrangler's `to_parquet()` internally calls `ListObjectsV2` before writing, which requires bucket-level permission, not just object-level.

**Fix:**
```json
{
  "Effect": "Allow",
  "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
  "Resource": "arn:aws:s3:::yt-pipeline-silver-dev"
},
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
  "Resource": "arn:aws:s3:::yt-pipeline-silver-dev/*"
}
```

**Lesson:** S3 IAM permissions are two-level: bucket ARN for list/location, and `bucket/*` ARN for object operations. Missing either one causes access denied.

---

### Bug 5 — Glue Job Name Mismatch in Step Functions

**Error:**
```
EntityNotFoundException: Job not found: yt-data-pipeline-bronze-to-silver-dev
```

**Root Cause:**
Step Functions was configured to call `yt-data-pipeline-bronze-to-silver-dev` (with `-dev` suffix) but the actual Glue job was named `yt-data-pipeline-bronze-to-silver` (no suffix).

**Fix:**
Updated the Step Functions state machine definition:
```json
"JobName": "yt-data-pipeline-bronze-to-silver"
```

**Lesson:** AWS resource names are exact strings — `job-name` and `job-name-dev` are completely different resources.

---

### Bug 6 — Lambda `KeyError` on Environment Variables When Called from Step Functions

**Error:**
```
KeyError: 'YOUTUBE_API_KEY'
File "/var/task/lambda_function.py", line 37, in <module>
    API_KEY = os.environ["YOUTUBE_API_KEY"]
```

**Root Cause:**
The env var was set correctly and direct Lambda invocation worked fine. But when called from Step Functions, the Lambda failed during Python module initialization — before `lambda_handler` was even called.

Reading `os.environ` at **module level** (outside any function) means it executes during the Lambda cold start / import phase. In certain Step Functions invocation contexts, environment variables are not yet injected at that point.

**Fix:**
Move all `os.environ` reads **inside** `lambda_handler`:
```python
# Before — module level, fails during cold start
API_KEY = os.environ["YOUTUBE_API_KEY"]  # line 37

# After — inside handler, always has env vars available
def lambda_handler(event, context):
    api_key = os.environ["YOUTUBE_API_KEY"]  # safe
    bucket  = os.environ["S3_BUCKET_BRONZE"]
    regions = os.environ.get("YOUTUBE_REGIONS", "US,GB,CA,IN").split(",")
```

**Lesson:** Never read environment variables at Lambda module level. Always read them inside `lambda_handler`. This is a well-known Lambda anti-pattern — module-level code runs during initialization, not at invocation time.

---

## Key Learnings

### 1. IAM is the backbone of every AWS service connection
Every service-to-service call requires explicit IAM permission. When debugging access errors, always verify both the bucket-level and object-level permissions separately.

### 2. AWS debugging sequence that works
```
1. Read the full error message carefully
2. Check that the resource actually exists (correct name/ARN)
3. Check IAM permissions
4. Check environment variable configuration
5. Check service-specific API limitations
6. Reproduce with a minimal test before fixing
```

### 3. Data format inspection before writing Spark jobs
Always download and inspect a raw Bronze file before writing any Spark read logic. The difference between NDJSON and a pretty-printed single JSON object is invisible from S3 metadata but completely breaks your job.

### 4. Module-level code in Lambda is dangerous
Lambda reuses execution environments. Module-level code runs once at cold start, not per invocation. Environment variables, connections, and any setup that could fail should live inside `lambda_handler` unless you're certain it's safe.

### 5. Test each component before end-to-end runs
Every Lambda and Glue job should be verified independently (direct invocation, manual Glue run) before wiring into Step Functions. Debugging failures in an orchestrator is much harder than debugging the component directly.

---

## Supported Regions

| Code | Country |
|---|---|
| US | United States |
| GB | United Kingdom |
| CA | Canada |
| IN | India |

> The pipeline supports up to 10 regions (DE, FR, JP, KR, MX, RU) — extend by updating the `YOUTUBE_REGIONS` Lambda environment variable.

---

## Data Sources

- **YouTube Data API v3** — live trending video data, fetched every 6 hours via EventBridge
- **Kaggle YouTube Trending Dataset** — historical data (2017–2018) for backfill and testing

---

## Author

Built as a hands-on data engineering portfolio project following the [Darshil Parmar YouTube tutorial](https://github.com/darshilparmar/youtube-data-piepline-aws-s3-lambda-glue-athena-stepfunction), with a key divergence: replaced static Kaggle CSV ingestion with a live YouTube Data API v3 Lambda, which introduced all the production debugging challenges documented above.
