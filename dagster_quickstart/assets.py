import json
import requests

import pandas as pd
import boto3
import os
import io

from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
    sensor,
    RunRequest,
    RunConfig,
    AssetSelection,
    op,
    job
)
from dagster_quickstart.configurations import HNStoriesConfig

@asset
def yesterdays_hackernews_stories(config: HNStoriesConfig):
    """Get the data"""
    s3 = boto3.client(
        service_name = 's3',
        endpoint_url = 'https://05aac85f0f9af317c65df97826af8962.r2.cloudflarestorage.com',
        aws_access_key_id = os.environ.get('R2_ACCESS_KEY_ID'),
        aws_secret_access_key = os.environ.get('R2_SECRET_ACCESS_KEY'),
        region_name = 'wnam'
    )

    obj = s3.get_object(Bucket='batch-monkey-demo', Key='top-hn-stories.snappy.parquet')
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

    # filter by date
    df['time'] = pd.to_datetime(df['time'])
    mask = (df['time'].dt.date == pd.to_datetime(config.hackernews_stories_date).date())
    date_filtered_df = df[mask]

    date_filtered_df.to_csv(config.hn_top_stories_path)


@asset(deps=[yesterdays_hackernews_stories])
def aviation_prompt_df(config: HNStoriesConfig):
    """Add the prompt column."""
    prompt_df = pd.read_csv(config.hn_top_stories_path)

    prompt_df = prompt_df[['id', 'title']]
    prompt_df['aviation_prompt'] = """Is the following news story related to aviation or not related to aviation? 
    The story has to be directly, explicitly related to aviation. 
    Provide a true or false answer, and then a justification for the answer. 
    Here is the title of the story: """ + prompt_df["title"]

    prompt_df.to_csv(config.stories_with_prompt_col)

@asset(deps=[aviation_prompt_df])
def upload_to_s3(config: HNStoriesConfig):
    s3 = boto3.client(
        service_name = 's3',
        endpoint_url = 'https://05aac85f0f9af317c65df97826af8962.r2.cloudflarestorage.com',
        aws_access_key_id = os.environ['R2_ACCESS_KEY_ID'],
        aws_secret_access_key = os.environ['R2_SECRET_ACCESS_KEY'],
        region_name = 'wnam'
    )

    df = pd.read_csv(config.stories_with_prompt_col)
    key = '/'.join([config.hackernews_stories_date, config.stories_with_prompt_col])

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Rewind the buffer
    bytes_buffer = io.BytesIO(csv_buffer.getvalue().encode())

    s3.upload_fileobj(bytes_buffer, config.s3_bucket, key)

@asset(deps=[upload_to_s3])
def start_inference_job(config: HNStoriesConfig):
    """Submit the job"""
    url = "https://sethkimmel3--batch-monkey-api-s3-batch-request.modal.run"

    read_key = '/'.join([config.hackernews_stories_date, config.stories_with_prompt_col])
    write_key = '/'.join([config.hackernews_stories_date, config.return_file])

    params = {
        "model_to_use": "mistral-aviation-json",
        "read_bucket": config.s3_bucket,
        "read_key": read_key,
        "prompt_column": "aviation_prompt",
        "write_bucket": config.s3_bucket,
        "write_key": write_key,
        "json_mode": True
    }
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.request("POST", url, headers=headers, data=json.dumps(params))
    print(response)

@asset
def do_something_with_inference_results(config: HNStoriesConfig):
    s3 = boto3.client(
        service_name = 's3',
        endpoint_url = 'https://05aac85f0f9af317c65df97826af8962.r2.cloudflarestorage.com',
        aws_access_key_id = os.environ['R2_ACCESS_KEY_ID'],
        aws_secret_access_key = os.environ['R2_SECRET_ACCESS_KEY'],
        region_name = 'wnam'
    )
    key = '/'.join([config.hackernews_stories_date, config.return_file])
    obj = s3.get_object(Bucket=config.s3_bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    for i in range(df.shape[0]):
        print(df.iloc[i]['title'])
        print(df.iloc[i]['generated_text'])
        print()

@sensor(
    asset_selection=AssetSelection.assets(do_something_with_inference_results)
)
def s3_sensor(context):
    s3 = boto3.client(
        service_name = 's3',
        endpoint_url = 'https://05aac85f0f9af317c65df97826af8962.r2.cloudflarestorage.com',
        aws_access_key_id = os.environ['R2_ACCESS_KEY_ID'],
        aws_secret_access_key = os.environ['R2_SECRET_ACCESS_KEY'],
        region_name = 'wnam'
    )

    if context.cursor == 'False' or context.cursor == None:
        resp = s3.list_objects_v2(Bucket='batch-monkey-demo')['Contents']
        all_keys = [item['Key'] for item in resp]

        print(all_keys)

        if '2023-09-15/hackernews_inference_results.csv' in all_keys:
            context.update_cursor('True')
            yield RunRequest(
                run_key="",
                run_config={}
            )
        else:
            context.update_cursor('False')