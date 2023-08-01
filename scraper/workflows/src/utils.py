BUCKET_NAME = "brasileirao-data"


def normalize_string(string: str) -> str:
    import re

    from unidecode import unidecode

    return unidecode(re.sub(r"[^\w\s]", "", string)).lower().strip().replace(" ", "")


def build_data_lake_path(
    date: str, home: str, away: str, week: int, filename: str, year: int
) -> str:
    import os

    return os.path.join(
        BUCKET_NAME,
        "raw",
        str(year),
        str(week),
        date.replace("-", ""),
        normalize_string(home) + "_" + normalize_string(away),
        filename,
    )
