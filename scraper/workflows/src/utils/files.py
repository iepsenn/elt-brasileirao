import json


def read_schemas_file():
    with open("scraper/workflows/src/schemas.json", "r") as file:
        return json.load(file)
