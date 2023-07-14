import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict


def get_content_from_url(url: str) -> BeautifulSoup:
    request = requests.get(url)
    page_content = BeautifulSoup(request.content, "html.parser")
    return page_content


def get_season_schedule(url: str) -> pd.DataFrame:
    """Receives a url of the page with the schedule table of seanson \
        and returns a table with the informations."""

    def get_match_details_page_url(page_content: BeautifulSoup) -> list:
        url_match_list = []
        rows = page_content.findAll("table")[0].findAll("tr")
        for row in rows:
            # Verify if the row contains useful information
            if not row.findAll("td", {"data-stat": "home_team"}):
                continue
            try:
                # Get the url to the page with details of the match
                match_url = row.findAll("td", {"class": "center"})[0].findAll(
                    "a", href=True
                )[0]["href"]
                url_match_list.append(match_url)
            except Exception:
                url_match_list.append(None)
        return list(
            map(
                lambda url: "https://fbref.com" + url
                if url is not None else url,
                url_match_list,
            )
        )

    page_content = get_content_from_url(url)
    match_schedule = pd.read_html(str(page_content.findAll("table")))[0]
    match_schedule["Url"] = get_match_details_page_url(page_content)

    match_schedule = match_schedule[
        ~match_schedule.Home.isna() & ~match_schedule.Away.isna()
    ]
    return match_schedule


def get_match_detail_tables(url: str) -> List[pd.DataFrame]:
    """Receives a url of the page with details of the match and returns \
        a tuple with the tables with statistics of the match."""
    table_indices = {
        0: "SummaryHome",
        1: "PassingHome",
        2: "PassTypesHome",
        3: "DefensiveActionsHome",
        4: "PossessionHome",
        5: "MiscellaneousStatsHome",
        6: "GoalkeeperHome",
        7: "SummaryAway",
        8: "PassingAway",
        9: "PassTypesAway",
        10: "DefensiveActionsAway",
        11: "PossessionAway",
        12: "MiscellaneousStatsAway",
    }

    def get_statistics_table(
        page_content: BeautifulSoup, table_index: int
    ) -> List[Dict[str, pd.DataFrame]]:
        return pd.read_html(
            str(page_content.findAll(
                "table", {"class": "stats_table"}
            )[table_index])
        )[0]

    tables = {}
    page_content = get_content_from_url(url)
    for index, table_name in table_indices.items():
        tables[table_name] = get_statistics_table(page_content, index)

    return tables
