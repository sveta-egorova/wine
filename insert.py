import argparse
import os, sys
from inserters import *
import mariadb
import settings
import pickle


def connect_to_vivino_db(): #TODO type annot
    """
    connect to vivino db and return a connection instance
    """
    try:
        conn = mariadb.connect(
                user="admin",
                password=settings.db_pass,
                host=settings.db_url,
                port=3306,
                database="vivino")
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        raise e
    return conn


# todo delete
# def fix_encoding(conn):
#     tables = ['wine_flavor_group', 'wine_keyword', 'keyword', 'vintage_toplist', 'toplist', 'vintage', 'wine',
#               'style_grape', 'style_food', 'grape', 'food', 'facts', 'price', 'winery', 'type', 'country', 'style',
#               'region', 'country_grape', 'user', 'review', 'activity', 'vintage_review']
#     try:
#         conn.cursor().execute("ALTER DATABASE vivino CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;")
#         for table in tables:
#             conn.cursor().execute(f"ALTER TABLE {table} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
#             print(f"enconding of table {table} converted")
#     finally:
#         conn.close()


def read_files_insert_to_sql(dir: str, inserters: List[Inserter], clean_first: bool, verbose: bool) -> None:
    if clean_first:
        for inserter in reversed(inserters):
            conn = connect_to_vivino_db()
            try:
                if verbose:
                    inserter.count_records(conn, 'Before cleaning')
                inserter.clean_table(conn)
            finally:
                conn.close()
    for inserter in inserters:
        print(f"Loading to {inserter.table}...")
        file_list = os.listdir(dir)
        for i in range(len(file_list)):
            if file_list[i].startswith(".") or os.path.isdir(os.path.join(dir, file_list[i])):
                pass
            else:
                with open(f'{dir}{file_list[i]}', 'rb') as f:
                    cur_data = pickle.load(f)
                    print(f"Loading {len(cur_data)} records from file {file_list[i]} (file {i+1} of {len(file_list)})...")
                conn = connect_to_vivino_db()
                try:
                    inserter.insert(conn, cur_data, verbose)
                finally:
                    conn.close()


    print("Loading complete.")


if __name__ == '__main__':
    """
    Usage: python insert.py [-w | -r] [-f] [-v] [-p PATH]
    """

    # parser = argparse.ArgumentParser(description='Write data to SQL database')
    # group = parser.add_mutually_exclusive_group()
    # group.add_argument("-w", "--wines", help="if wines should be loaded to SQL", action="store_true")
    # group.add_argument("-r", "--reviews", help="if reviews should be loaded to SQL", action="store_true")
    # parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    # parser.add_argument("-c", "--clean", help="cleans table before inserting", action="store_true")
    # parser.add_argument("-p", "--path", help="path to load data", default="backup_data/")
    #
    # args = parser.parse_args()
    #
    # store_wines = args.wines
    # store_reviews = args.reviews
    # verbose = args.verbose
    # clean = args.clean
    # backup_dir = args.path

    store_wines = False
    store_reviews = True
    verbose = True
    clean = False
    backup_dir = "backup_data/reviews/France/"

    mapping = {
        'wines': [TypeInserter(), WineryInserter(), CountryInserter(), RegionInserter(), StyleInserter(),
                  FoodInserter(), FactInserter(), StyleFoodInserter(), GrapeInserter(), StyleGrapeInserter(),
                  CountryGrapeInserter(), WineInserter(), PriceInserter(), VintageInserter(), ToplistInserter(),
                  VintageToplistInserter()],
        'reviews': [ReviewInserter(), VintageReviewInserter()] #[UserInserter(), ActivityInserter(),
    }

    inserters = []
    if store_wines:
        # backup_dir = backup_dir  # todo maybe delete
        inserters.extend(mapping['wines'])
    elif store_reviews:
        # backup_dir = backup_dir + 'reviews/'  # todo maybe delete
        inserters.extend(mapping['reviews'])

    read_files_insert_to_sql(backup_dir, inserters, clean, verbose)















    # for directory, inserters in mapping:
    #     # for inserter in inserters:
    #     read_files_insert_to_sql(base_dir + directory, inserters,  # batch_size, first_entry, verbose)
    #
    #                              base_dir = 'backup_data/'
    # os.mkdir(base_dir)
    # row_vintages = download_vintages(0, 400)  # vintages a.k.a matches
    # dump_wines(base_dir + '/explore/all_wines.pickle', row_vintages)
    # save_vintages_to_db(row_vintages)
    #
    # vintages = flatten_and_drop_columns(row_vintages)
    #
    # for country in ['Italy', 'France']:
    #     available_years = extract_distinct_years(vintages)
    #     for year in available_years:
    #         wines = filter_by_country_and_year(vintages, year, country)
    #         session = requests.Session()
    #         reviews = []
    #         for wine in wines:
    #             reviews.extend(download_reviews(session, wine))
    #         dump_reviews(f'{base_dir}/reviews/{country}_{year}.pickle', reviews)
#             save_reviews_to_db(reviews)
#
#
#
#
# def download_vintages(price_min: int, price_max: int) -> List[Dict]:
#     pass
#
#
# def dump_wines(file_path: str, vintages: List[Dict]) -> None:
#     pass
#
#
# def save_vintages_to_db(vintages: List[Dict]) -> None:
#     pass
#
#
# def flatten_and_drop_columns(vintages: List[Dict]) -> DataFrame:
#     pass
#
#
# def extract_distinct_years(vintages: DataFrame) -> Series:
#     pass
#
#
# def filter_by_country_and_year(vintages: DataFrame, year: str, country: str) -> DataFrame:
#     pass
#
#
# def download_reviews(session: requests.Session, wine: DataFrame) -> List[Dict]:
#     pass
#
#
# def dump_reviews(file_path: str, reviews: List[Dict]) -> None:
#     pass
#
#
# def save_reviews_to_db(reviews: List[Dict]) -> None:
#     pass
