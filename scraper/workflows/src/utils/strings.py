import os
import re

from unidecode import unidecode

from .environment import BUCKET_NAME


def normalize_string(string: str) -> str:
    return unidecode(re.sub(r"[^\w\s]", "", string)).lower().strip().replace(" ", "")


def build_data_lake_path(
    date: str, home: str, away: str, week: int, filename: str, year: int
) -> str:
    return os.path.join(
        BUCKET_NAME,
        "raw",
        str(year),
        str(week),
        date.replace("-", ""),
        normalize_string(home) + "_" + normalize_string(away),
        filename,
    )
