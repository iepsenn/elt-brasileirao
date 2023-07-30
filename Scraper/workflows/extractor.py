import os
import s3fs
import pandas as pd
from time import sleep

from source.storage import get_s3_client, load_to_bucket
from source.scraper import (
    get_match_detail_tables,
    get_season_schedule,
)
from source.utils import build_data_lake_path
from source.utils import BUCKET_NAME

from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def load_match_details_to_storage(
    match_info: pd.Series,
    s3_client: s3fs.S3FileSystem
):
    if match_info.Url is None:
        return

    # Build the storage path in the format:
    # <BUCKET_NAME>/raw/<SEASON_YEAR>/<GAME_WEEK>/"%y%m%d"/<HOME_TEAM>_<AWAY_TEAM>
    storage_base_path = build_data_lake_path(
        year=int(match_info.Date.split("-")[0]),
        week=int(match_info.Wk),
        date=match_info.Date,
        home=match_info.Home,
        away=match_info.Away,
        filename="",
    )

    match_detail_tables = get_match_detail_tables(
        url=match_info.Url,
        week=match_info.Wk,
        date=match_info.Date,
        team_home=match_info.Home,
        team_away=match_info.Away,
    )
    for filename, table in match_detail_tables.items():
        file_path = os.path.join(storage_base_path, f"{filename}.csv")
        print(f"Loading file to {file_path}.")
        load_to_bucket(
            s3_client=s3_client,
            dataframe=table,
            storage_path=file_path,
        )

    sleep(2)
    return


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
    retries=3,
    retry_delay_seconds=60
)
def extract_season_schedule(url: str):
    # Get match detail tables
    return get_season_schedule(url)


@task(retries=3, retry_delay_seconds=60)
def load_season_schedule_metadata_to_datalake(
    client: s3fs.S3FileSystem,
    data: pd.DataFrame,
    season_year: int
):
    # Save season schedule metadata
    load_to_bucket(
        s3_client=client,
        dataframe=data,
        storage_path=os.path.join(
            BUCKET_NAME, "raw", f"season_{season_year}.csv"
        ),
    )


@flow(retries=3, retry_delay_seconds=90, log_prints=True)
def extract_season_data(season_url: str):
    s3_client = get_s3_client()

    # Get match detail tables
    df = extract_season_schedule(season_url)
    season_year = df.iloc[0].Date.split("-")[0]
    print(f"Extracting data from season {season_year}")

    # Save every match detail tables of the season by
    # url column of the metadata table
    for game_week in range(1, df.Wk.astype(int).max() + 1):
        # Split to process by game week concurrently
        print(f"Extracting matches from game week {game_week}")
        df[df.Wk == game_week].apply(
            lambda x: load_match_details_to_storage.submit(x, s3_client),
            axis=1
        )
        break  # to test

    # Save season schedule metadata
    load_season_schedule_metadata_to_datalake(
        client=s3_client,
        data=df,
        season_year=season_year
    )


if __name__ == "__main__":
    # example
    extract_season_data(
        "https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures"
    )  # noqa
