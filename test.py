import os
import sys

from crawlers import *
import pandas as pd


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


def filter_wines(wine_list, country, year):
    full_df = pd.DataFrame(remove_wine_duplicates(wine_list))
    full_df = pd.json_normalize(full_df['vintage'])
    selected_columns = full_df[full_df['has_valid_ratings'] == True][['id', 'year', 'statistics.ratings_count',
                                                                      'wine.id', 'wine.region.country.name']]
    selected_columns.columns = ['id', 'year', 'rating_count', 'wine_id', 'country']
    data = selected_columns[(selected_columns['country'] == country) &
                            (selected_columns['year'] == year)]\
        .sort_values('rating_count', ascending=False)
    return data


def read_and_filter_wines(path, country, year):
    with open(path, 'rb') as f:
        recovered_data = pickle.load(f)
    return filter_wines(recovered_data, country, year)


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
    Usage: python test.py France 2011:2013 False
    """

    country = sys.argv[1]
    years_string = sys.argv[2]
    verbose = bool(sys.argv[3])

    # country = 'France'
    # years_string = '2013:2015'
    # verbose = False

    backup_dir = "backup_data/"
    years = parse_years(years_string)

    crawler = Crawler(backup_dir=backup_dir, verbose=verbose)

    for year in years:
        if os.path.isfile(backup_dir + "full_match_list"):
            wines = read_and_filter_wines(backup_dir + "full_match_list", country, year)
        else:
            wines = crawler.download_all_wines(105, 107, with_prices=True, inter_backup=False, final_backup=False)
            wines = filter_wines(wines, country, year)

        if len(wines) > 0:
            reviews = crawler.download_reviews(wines, country, year)

            # reviews = read_and_preprocess_reviews(country, year)  # TODO read_and_preprocess_reviews must include 2 calls: read and preprocess
            reviews = deduplicate_and_filter_reviews(reviews, year)
            save_reviews(reviews, backup_dir + 'reviews/', country, year)

            print(f"After processing, the data on {country} in {year} includes {reviews['id'].nunique()} unique reviews")


    # print(reviews.groupby('vintage.year').count()['id'])
    # print(f"Hey, I extracted a dataframe of shape {reviews.shape}")
    # print(reviews.iloc[:5,:5])

    #
    # wine_crawler = Crawler(s)
    # results = wine_crawler.load_all_wines(105, 107, inter_backup=False, final_backup=False, verbose=True)

    # data_filtered = filter_by_country_year(data, 'France', 2013)
    # print(f"Hey, I extracted a dataframe of shape {data_filtered.shape}")
    # print(data_filtered.head())



#     with open(f"backup_data/full_match_list", 'rb') as f:
#         recovered_data = pickle.load(f)
#
#
#     def remove_wine_duplicates(json_data):
#         distinct_dict = {entry['vintage']['id']: entry for entry in json_data}
#         recovered_data_distinct = distinct_dict.values()
#         return list(recovered_data_distinct)
#
#
#     recovered_data_distinct = remove_wine_duplicates(recovered_data)
#
#     full_df = pd.DataFrame(recovered_data_distinct)
#     full_df_normalized = pd.json_normalize(full_df['vintage'])
#
#     review_filtering_df = full_df_normalized[
#         ['id', 'year', 'has_valid_ratings', 'statistics.ratings_count', 'wine.id', 'wine.region.country.name']]
#
#     filter_df = review_filtering_df.loc[
#         review_filtering_df['has_valid_ratings'] == True, ['id', 'year', 'statistics.ratings_count',
#                                                            'wine.id', 'wine.region.country.name']]
#
#     filter_df.columns = ['id', 'year', 'reviews_count', 'wine_id', 'country']
#
#     data_Italy_2015 = filter_df[(filter_df['country'] == 'Italy') & (filter_df['year'] == 2015)].sort_values(
#         'reviews_count', ascending=False)

# data_Italy_2015.head(20)

