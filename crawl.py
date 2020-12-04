import os
import sys

from crawlers import *
import pandas as pd
import argparse



def parse_years(years_string: str):
    if years_string.startswith(':'):
        year_start = 1900
    else:
        year_start = int(years_string[:4])
    if years_string.endswith(':'):
        year_end = 2025
    else:
        year_end = int(years_string[-4:])
    return range(year_start, year_end + 1)


def remove_wine_duplicates(json_data):
    distinct_dict = {entry['vintage']['id']: entry for entry in json_data}
    data_distinct = distinct_dict.values()
    return list(data_distinct)


def remove_review_duplicates(json_data):
    distinct_dict = {entry['id']: entry for entry in json_data}
    data_distinct = distinct_dict.values()
    return list(data_distinct)


def wines_as_df(wine_list):
    full_df = pd.DataFrame(remove_wine_duplicates(wine_list))
    full_df = pd.json_normalize(full_df['vintage'])
    selected_columns = full_df[full_df['has_valid_ratings'] == True][['id', 'year', 'statistics.ratings_count',
                                                                      'wine.id', 'wine.region.country.name']]
    selected_columns.columns = ['id', 'year', 'rating_count', 'wine_id', 'country']
    return selected_columns


def read_wines_to_df(path):
    with open(path, 'rb') as f:
        recovered_data = pickle.load(f)
    return wines_as_df(recovered_data)


def filter_wines(df, country, year):
    data = df[(df['country'] == country) & (df['year'] == year)].sort_values('rating_count', ascending=False)
    return data


def deduplicate_and_filter_reviews(review_list, year):
    full_df = pd.json_normalize(remove_review_duplicates(review_list))
    selected_columns = full_df[(full_df['vintage.year'] == year)]
    # selected_columns.columns = ['id', 'year', 'rating_count', 'wine_id', 'country']
    return selected_columns


def save_reviews(review_list, backup_dir, country, year):
    with open(f"{backup_dir}{country}_{year}", 'wb') as f:
        pickle.dump(review_list, f)


if __name__ == "__main__":
    """
    Usage: python crawl.py [-h] [-v] country years
    """

    parser = argparse.ArgumentParser(description='Load some wine reviews')
    parser.add_argument("country", type=str, help="Input the country name, eg. France")
    parser.add_argument("years", type=str, help="Input the year or year range, eg. 2005:2010")
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    args = parser.parse_args()

    country = args.country
    years_string = args.years
    verbose = args.verbose

    # country = 'France'
    # years_string = '2013:2015'
    # verbose = False

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

            reviews = deduplicate_and_filter_reviews(reviews, year)
            save_reviews(reviews, backup_dir + 'reviews/', country, year)

            if verbose:
                print(f"After processing, the data on {country} in {year} includes {reviews['id'].nunique()} "
                      f"unique reviews on {reviews['vintage.wine.id'].nunique()} wines")
