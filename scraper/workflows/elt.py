from src.extractor import extract_season_data
from src.loader import load_data

from prefect import flow

seasons_to_extract = {
    2022: "https://fbref.com/en/comps/24/2022/schedule/2022-Serie-A-Scores-and-Fixtures"
}


@flow(retries=3, retry_delay_seconds=90, log_prints=True)
def elt():
    for year, url in seasons_to_extract.items():
        extract_season_data(url)
        load_data(year)


if __name__ == "__main__":
    elt()
