import os
import sys

from crawlers import *
import requests
import pandas as pd


def remove_wine_duplicates(json_data):
    distinct_dict = {entry['vintage']['id']: entry for entry in json_data}
    data_distinct = distinct_dict.values()
    return list(data_distinct)


def remove_review_duplicates(json_data):
    distinct_dict = {entry['id']: entry for entry in json_data}
    data_distinct = distinct_dict.values()
    return list(data_distinct)


def read_and_preprocess_wines(path, country, year):
    with open(path, 'rb') as f:
        recovered_data = pickle.load(f)
    full_df = pd.DataFrame(remove_wine_duplicates(recovered_data))
    full_df = pd.json_normalize(full_df['vintage'])
    selected_columns = full_df[full_df['has_valid_ratings'] == True]\
        [['id', 'year', 'statistics.ratings_count', 'wine.id', 'wine.region.country.name']]
    selected_columns.columns = ['id', 'year', 'rating_count', 'wine_id', 'country']
    data = selected_columns[(selected_columns['country'] == country) &
                            (selected_columns['year'] == year)]\
        .sort_values('rating_count', ascending=False)
    return data


def read_and_preprocess_reviews(country, year, backup_directory="backup_data/reviews/"):
    with open(f"{backup_directory}{country}_{year}", 'rb') as f:
        recovered_data = pickle.load(f)
    full_df = pd.json_normalize(remove_review_duplicates(recovered_data))
    selected_columns = full_df[(full_df['vintage.wine.region.country.name'] == country)
                               & (full_df['vintage.year'] == year)]
    # selected_columns.columns = ['id', 'year', 'rating_count', 'wine_id', 'country']
    return selected_columns

# def filter_by_country_year(dataframe, country: str, year: str):
#     data = dataframe[(dataframe['country'] == country) & (dataframe['year'] == year)]
#     return data.sort_values('rating_count', ascending=False)


# data_Italy_2014 = filter_df[(filter_df['country'] == 'Italy') & (filter_df['year'] == 2014)].sort_values('reviews_count', ascending=False)
#
#

if __name__ == "__main__":
    """
    Usage: python test.py France 2019
    """

    country = sys.argv[1]
    year = int(sys.argv[2])

    crawler = Crawler()

    full_list_file = "backup_data/full_match_list"
    if os.path.isfile(full_list_file):
        wines = read_and_preprocess_wines(full_list_file, country, year)
    else:
        wines = crawler.download_all_wines(105, 107, inter_backup=False, final_backup=False, verbose=True)
        save_wines(full_list_file, wines)
        wines = preprocess_wines(wines, country, year)

    reviews = crawler.download_reviews(wines)
    s.close()
    save_reviews("backup_directory/bla/", country, year, reviews)

    # reviews = read_and_preprocess_reviews(country, year)  # TODO read_and_preprocess_reviews must include 2 calls: read and preprocess
    reviews = deduplicate_and_filter_reviews(reviews, year)
    print(f"After processing, the data on {country} and {year} includes {reviews['id'].nunique()} unique reviews")



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

