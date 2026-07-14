Bronze bucket name  - yt-piepline-ap-south-bronze-dev
silver bucket name = yt-piepline-ap-south-silver-dev
gold layer name - yt-piepline-ap-south-gold-dev
Script Bucket - yt-piepline-ap-south-scripts-dev

GLUE_DB_BRONZE - yt_pipeline_broze_dev
GLUE_DB_SILVER - yt_pipeline_silver_dev
GLUE_DB_GOLD - yt_pipeline_gold_dev

--bronze_database-yt_pipeline_broze_dev
--bronze_table - raw_stastics
--silver_bucket - yt-piepline-ap-south-silver-dev

--silver_database: yt_pipeline_silver_dev
--silver_table: clean_stastics
--gold_bucket: yt-piepline-ap-south-gold-dev
--gold_database:yt_pipeline_gold_dev

SNS ARN -  arn:aws:sns:ap-south-1:916960894342:yt-data-pipeline-alerts_s

Lambda conversion notes
- The Lambda handler is added at [scripts/lambda_convert_to_parquet.py](scripts/lambda_convert_to_parquet.py#L1).
- Requirements for local build / layer are in [scripts/requirements-lambda.txt](scripts/requirements-lambda.txt#L1).
- Environment variables:
	- `SOURCE_BUCKET` (default: `yt-piepline-ap-south-silver-dev`)
	- `SOURCE_PREFIX` (default: `''`)
	- `DEST_BUCKET` (default: same as `SOURCE_BUCKET`)
	- `PARQUET_PREFIX` (default: `parquet/`)
- Deployment options:
	- Create a Lambda Layer containing `pandas` and `pyarrow` (recommended), or
	- Build a container image with the dependencies and deploy to Lambda, or
	- Use an AWS Glue job (no packaging required) for large datasets.

Quick deploy steps (container or layer required for pyarrow):
1. Package dependencies into a Lambda Layer or build a container image including `requirements-lambda.txt`.
2. Upload the handler `scripts/lambda_convert_to_parquet.py` as the Lambda function entry (handler: `lambda_convert_to_parquet.lambda_handler`).
3. Provide the Lambda IAM role `s3:GetObject`, `s3:PutObject`, and `s3:ListBucket` permissions on the source/dest buckets.
4. Invoke the function with an event JSON specifying `{"source_bucket":"<bucket>","source_prefix":"<prefix>"}` or set env vars.
