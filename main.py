import os
from typing import Dict, List
import requests
from pandas import DataFrame, Series
from inserters import *


def download_vintages(price_min: int, price_max: int) -> List[Dict]:
    pass


def dump_wines(file_path: str, vintages: List[Dict]) -> None:
    pass


def save_vintages_to_db(vintages: List[Dict]) -> None:
    pass


def flatten_and_drop_columns(vintages: List[Dict]) -> DataFrame:
    pass


def extract_distinct_years(vintages: DataFrame) -> Series:
    pass


def filter_by_country_and_year(vintages: DataFrame, year: str, country: str) -> DataFrame:
    pass


def download_reviews(session: requests.Session, wine: DataFrame) -> List[Dict]:
    pass


def dump_reviews(file_path: str, reviews: List[Dict]) -> None:
    pass


def save_reviews_to_db(reviews: List[Dict]) -> None:
    pass


if __name__ == '__main__':
    base_dir = 'backup_data'
    os.mkdir(base_dir)

    row_vintages = download_vintages(0, 400)  # vintages a.k.a matches
    dump_wines(base_dir + '/explore/all_wines.pickle', row_vintages)
    save_vintages_to_db(row_vintages)

    vintages = flatten_and_drop_columns(row_vintages)

    for country in ['Italy', 'France']:
        available_years = extract_distinct_years(vintages)
        for year in available_years:
            wines = filter_by_country_and_year(vintages, year, country)
            session = requests.Session()
            reviews = []
            for wine in wines:
                reviews.extend(download_reviews(session, wine))
            dump_reviews(f'{base_dir}/reviews/{country}_{year}.pickle', reviews)
            save_reviews_to_db(reviews)
