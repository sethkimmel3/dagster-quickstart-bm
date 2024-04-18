from dagster import Config


class HNStoriesConfig(Config):
    stories_with_prompt_col: str = "hackernews_stories_with_prompt.csv"
    hn_top_stories_path: str = "hackernews_top_stories.csv"
    hackernews_stories_date: str = '2023-09-15'
    s3_bucket: str = 'batch-monkey-demo'
    return_file: str = "hackernews_inference_results.csv"
