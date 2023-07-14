import os
import s3fs
import logging
from time import sleep
import pandas as pd

from source.storage import get_s3_client, load_to_bucket
from source.scrapper import (
    get_match_detail_tables,
    get_season_schedule,
)
from source.utils import build_data_lake_path
from source.utils import BUCKET_NAME


def extract_season_data(season_url: str):
    s3_client = get_s3_client()

    def load_match_details_to_storage(
        match_info: pd.Series,
        s3_client: s3fs.S3FileSystem
    ):
        if match_info.Url is None:
            return

        # Build the storage path in the format:
        # <BUCKET_NAME>/raw/<GAME_WEEK>/"%y%m%d"/<HOME_TEAM>_<AWAY_TEAM>
        storage_base_path = build_data_lake_path(
            date=match_info.Date,
            home=match_info.Home,
            away=match_info.Away,
            week=int(match_info.Wk),
            filename="",
        )

        match_detail_tables = get_match_detail_tables(match_info.Url)
        for filename, table in match_detail_tables.items():
            file_path = os.path.join(storage_base_path, f"{filename}.csv")
            logging.info(f"Loading file to {file_path}.")
            load_to_bucket(
                s3_client=s3_client,
                dataframe=table,
                storage_path=file_path,
            )

        sleep(2)
        return

    # Get match detail tables
    df = get_season_schedule(season_url)
    season_year = df.iloc[0].Date.split('-')[0]
    logging.info(f"Extracting data from season {season_year}")

    # Save every match detail tables of the season by
    # url column of the metadata table
    for game_week in range(1, df.Wk.astype(int).max() + 1):
        # Splt to process by game week
        logging.info(f"Extracting matches from game week {game_week}")
        df[df.Wk == game_week].apply(
            lambda x: load_match_details_to_storage(x, s3_client),
            axis=1
        )
        break

    # Save season schedule metadata
    load_to_bucket(
        s3_client=s3_client,
        dataframe=df,
        storage_path=os.path.join(
            BUCKET_NAME,
            "raw",
            filename=f"season_{season_year}.csv"
        ),
    )


if __name__ == "__main__":
    # example
    extract_season_data("https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures")  # noqa
