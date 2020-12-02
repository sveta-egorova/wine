

if __name__ == "__main__":
    import pickle
    import pandas as pd

    with open(f"backup_data/full_match_list", 'rb') as f:
        recovered_data = pickle.load(f)


    def remove_wine_duplicates(json_data):
        distinct_dict = {entry['vintage']['id']: entry for entry in json_data}
        recovered_data_distinct = distinct_dict.values()
        return list(recovered_data_distinct)


    recovered_data_distinct = remove_wine_duplicates(recovered_data)

    full_df = pd.DataFrame(recovered_data_distinct)
    full_df_normalized = pd.json_normalize(full_df['vintage'])

    review_filtering_df = full_df_normalized[
        ['id', 'year', 'has_valid_ratings', 'statistics.ratings_count', 'wine.id', 'wine.region.country.name']]

    filter_df = review_filtering_df.loc[
        review_filtering_df['has_valid_ratings'] == True, ['id', 'year', 'statistics.ratings_count',
                                                           'wine.id', 'wine.region.country.name']]

    filter_df.columns = ['id', 'year', 'reviews_count', 'wine_id', 'country']

    data_Italy_2015 = filter_df[(filter_df['country'] == 'Italy') & (filter_df['year'] == 2015)].sort_values(
        'reviews_count', ascending=False)

#%%

data_Italy_2015.head(20)

#%%


