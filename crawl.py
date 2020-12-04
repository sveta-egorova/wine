import os
from crawlers import *
import pandas as pd
import argparse
from typing import List, Dict, Union

Vintage = Union[str, int]

def parse_years(years_string: str) -> range:
    """
    Function that inputs a string with the year range, and returns a Python range
    :param years_string: The string specifying the desired year range, eg. '2012:2015:
    :return: sequence of integers
    """
    if years_string.startswith(':'):
        year_start = 1900
    else:
        year_start = int(years_string[:4])
    if years_string.endswith(':'):
        year_end = 2025
    else:
        year_end = int(years_string[-4:])
    return range(year_start, year_end + 1)


def remove_wine_duplicates(json_data: List[Dict]) -> List[Dict]:
    """
    Function that eliminates duplicates from the list of wines
    :param json_data: list of dictionaries containing wine data with duplicates
    :return: list of dictionaries containing wine data without duplicates
    """
    distinct_dict = {entry['vintage']['id']: entry for entry in json_data}
    data_distinct = distinct_dict.values()
    return list(data_distinct)


def remove_review_duplicates(json_data: List[Dict]) -> List[Dict]:
    """
    Function that eliminates duplicates from the list of reviews
    :param json_data: list of dictionaries containing review data with duplicates
    :return: list of dictionaries containing review data without duplicates
    """
    distinct_dict = {entry['id']: entry for entry in json_data}
    data_distinct = distinct_dict.values()
    return list(data_distinct)


def wines_as_df(wine_list: List[Dict]) -> pd.DataFrame:
    """
    Function that removes duplicates and selects specific columns needed for further filtering.
    :param wine_list: list of dictionaries containing wine data with duplicates
    :return: Pandas DataFrame with wine data
    """
    full_df = pd.DataFrame(remove_wine_duplicates(wine_list))
    full_df = pd.json_normalize(full_df['vintage'])
    selected_columns = full_df[full_df['has_valid_ratings'] == True][['id', 'year', 'statistics.ratings_count',
                                                                      'wine.id', 'wine.region.country.name']]
    selected_columns.columns = ['id', 'year', 'rating_count', 'wine_id', 'country']
    return selected_columns


def read_wines_to_df(path: str) -> pd.DataFrame:
    """
    Function that reads pickle file and returns a DataFrame with selected columns on wine data
    :param path: path to pickle file storing data
    :return: Pandas DataFrame with wine data
    """
    with open(path, 'rb') as f:
        recovered_data = pickle.load(f)
    return wines_as_df(recovered_data)


def filter_wines(df: pd.DataFrame, country: str, year: Vintage) -> pd.DataFrame:
    """
    Function that filters a dataframe by country and year
    :param df: Pandas DataFrame with wine data
    :param country: Country name
    :param year: A year for filtering data
    :return: filtered Pandas DataFrame with wine data
    """
    data = df[(df['country'] == country) & (df['year'] == year)].sort_values('rating_count', ascending=False)
    return data


def deduplicate_and_filter_reviews(review_list: List[Dict], year: Vintage) -> pd.DataFrame:
    """
    Function tht checks that DataFrame contains only specific values for year, and deletes records otherwise
    :param review_list: list of dictionaries containing review data
    :param year: A year for filtering data
    :return: filtered Pandas DataFrame with review data
    """
    full_df = pd.json_normalize(remove_review_duplicates(review_list))
    selected_columns = full_df[(full_df['vintage.year'] == year)]
    # selected_columns.columns = ['id', 'year', 'rating_count', 'wine_id', 'country']
    return selected_columns


def save_reviews(review_list: List[Dict], backup_dir: str, country: str, year: Vintage) -> None:
    """
    Function that dumps reviews as JSON object to a pickle file in the desired directory with a name specifying country and year
    :param review_list: list of dictionaries containing review data
    :param backup_dir: name of directory that should contain review data
    :param country: Country name for the batch of reviews
    :param year: Year for the batch of reviews
    :return: None
    """
    with open(f"{backup_dir}{country}_{year}", 'wb') as f:
        pickle.dump(review_list, f)


if __name__ == "__main__":
    """
    Usage: python crawl.py [-h] [-v] country years
    """
    #
    # parser = argparse.ArgumentParser(description='Load some wine reviews')
    # parser.add_argument("country", type=str, help="Input the country name, eg. France")
    # parser.add_argument("years", type=str, help="Input the year or year range, eg. 2005:2010")
    # parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    # args = parser.parse_args()
    #
    # country = args.country
    # years_string = args.years
    # verbose = args.verbose

    country = 'France'
    years_string = '1937'
    verbose = True

    backup_dir = "backup_data/"
    years = parse_years(years_string)

    crawler = Crawler(backup_dir=backup_dir, verbose=verbose)

    if os.path.isfile(backup_dir + "full_match_list"):
        wines_df = read_wines_to_df(backup_dir + "full_match_list")
    else:
        wines = crawler.download_all_wines(105, 107, with_prices=True, inter_backup=False, final_backup=False)
        wines_df = wines_as_df(wines)

    for year in years:
        wines = filter_wines(wines_df, country, year)

        if len(wines) > 0:
            if verbose:
                print(f"Loading year {year}, with {len(wines)} wines")

            reviews = crawler.download_reviews(wines, country, year)
            save_reviews(reviews, backup_dir + 'reviews/', country, year)

            if verbose:
                reviews_df = deduplicate_and_filter_reviews(reviews, year)
                print(f"After processing, the data on {country} in {year} includes {reviews_df['id'].nunique()} "
                      f"unique reviews on {reviews_df['vintage.wine.id'].nunique()} wines")
