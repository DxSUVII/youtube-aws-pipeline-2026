import os
import io
import logging
import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


def list_csv_objects(bucket: str, prefix: str):
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.lower().endswith('.csv'):
                yield key


def convert_csv_obj_to_parquet(bucket: str, key: str):
    logger.info('Reading s3://%s/%s', bucket, key)
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp['Body'].read()

    df = pd.read_csv(io.BytesIO(body))

    out_buffer = io.BytesIO()
    # Write Parquet using pyarrow engine
    df.to_parquet(out_buffer, engine='pyarrow', index=False)
    out_buffer.seek(0)

    return out_buffer


def lambda_handler(event, context):
    # Accept overrides from the event or environment
    source_bucket = event.get('source_bucket') or os.environ.get('SOURCE_BUCKET') or 'yt-piepline-ap-south-silver-dev'
    source_prefix = event.get('source_prefix') or os.environ.get('SOURCE_PREFIX') or ''
    dest_bucket = event.get('dest_bucket') or os.environ.get('DEST_BUCKET') or source_bucket
    parquet_prefix = event.get('parquet_prefix') or os.environ.get('PARQUET_PREFIX') or 'parquet/'

    logger.info('Source: s3://%s/%s', source_bucket, source_prefix)
    logger.info('Destination: s3://%s/%s', dest_bucket, parquet_prefix)

    processed = []
    for key in list_csv_objects(source_bucket, source_prefix):
        try:
            out_buffer = convert_csv_obj_to_parquet(source_bucket, key)

            # Build destination key
            base_name = os.path.splitext(os.path.basename(key))[0] + '.parquet'
            dest_key = parquet_prefix.rstrip('/') + '/' + base_name

            logger.info('Uploading parquet to s3://%s/%s', dest_bucket, dest_key)
            s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=out_buffer.getvalue())
            processed.append({'source': key, 'dest': dest_key})
        except Exception as e:
            logger.exception('Failed processing %s: %s', key, e)

    return {
        'status': 'completed',
        'processed_count': len(processed),
        'files': processed,
    }


if __name__ == '__main__':
    # Local quick-run helper
    import json
    evt = {
        'source_bucket': os.environ.get('SOURCE_BUCKET', ''),
        'source_prefix': os.environ.get('SOURCE_PREFIX', 'data/'),
        'parquet_prefix': os.environ.get('PARQUET_PREFIX', 'parquet/')
    }
    print(json.dumps(lambda_handler(evt, None), indent=2))
