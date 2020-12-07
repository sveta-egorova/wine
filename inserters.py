from abc import ABC
import sys, os
from typing import List
import time
import math


class Inserter(ABC):

    def __init__(self, table, paths=[], prefix="", pk_sql=[], batch_size=60000):
        self.table = table
        self.prefix = prefix
        self.paths = [prefix + path for path in paths]
        self.pk_sql = pk_sql
        self.batch_size = batch_size

    def _extract_json_to_sql(self, conn, matches_list, first_entry, verbose):
        """
        Function that accepts the folowing arguments:
        * conn: active connection to a database
        * matches_list: JSON list with data,
        * from_list_with_id: defines if data is stored as a list inside JSON with some ID stored outside that list
        * first_entry: boolean indicating whether it's the first time data is written to the table
        * verbose: prints the progress of loading

        Function inserts data and (for the first entry) checks whether the resulting number of unique records in SQL
        matches the number of unique records in JSON.

        """
        cur = conn.cursor()
        timepoint_1 = time.time()
        all_args = self.extract(matches_list)

        # part of the query that tells to do nothing on duplicate keys if such entry already exists,
        # depending on the number of primary keys
        if len(self.pk_sql) == 1:
            if_duplicates_do_nothing = f" ON DUPLICATE KEY UPDATE {self.pk_sql[0]} = {self.pk_sql[0]}"
        elif len(self.pk_sql) == 2:
            if_duplicates_do_nothing = f" ON DUPLICATE KEY UPDATE {self.pk_sql[0]} = {self.pk_sql[0]}, " \
                                       f"{self.pk_sql[1]} = {self.pk_sql[1]}"
        else:
            if_duplicates_do_nothing = ""

        query = f"""
                INSERT INTO {self.table} VALUES ({', '.join('?' * self.fields_num())})
                {if_duplicates_do_nothing}
            """.strip()
        cur.executemany(query, list(all_args.values()))
        conn.commit()

        timepoint_2 = time.time()
        if verbose:
            print(f'Insertion to {self.table} complete and took {timepoint_2 - timepoint_1} s.')

        if first_entry:
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) FROM {}'.format(self.table))
            unique_sql = cur.fetchall()[0][0]  # counts unique entries in the table after the insert statement
            unique_python = len(list(all_args.values()))  # counts unique entries in JSON
            if unique_python == unique_sql:
                print('Number of unique records is accurate')
            else:
                print('Something went wrong')

    def extract(self, matches_list):
        all_args = {}
        for entry in matches_list:
            # here, each entry represents a Python dictionary
            if from_list_with_id:
                # if data is passed from json list, entry contains a tuple (id, relevant_data, named entry[0]
                # and entry[1], which need to be unpacked in a single arg list

                values_entry = [entry[0]] + [int_to_float(get_value(entry[1], path)) for path in self.paths]
                if len(self.paths) == 0:
                    for record in entry[1]:
                        all_args[(entry[0], record)] = (entry[0], record)
            else:
                values_entry = [int_to_float(get_value(entry, path)) for path in self.paths]
            pk_values = values_entry[:len(self.pk_sql)]  # provided primary keys always come first

            #  We'd like to make sure that no duplicates are added to the database from our batch.
            #  So we create an argument dictionary for each specific set of primary keys (provided that
            #  all primary keys are not Null).
            #  The exception is a Facts table with no primary keys (since it doesn't have any paths, we exclude it
            #  by setting len(self.paths) > 0 for this check
            if all(pk_value is not None for pk_value in pk_values) and len(self.paths) > 0:
                all_args[tuple(pk_values)] = tuple(values_entry)

        return all_args

    def insert(self, conn, matches, first_entry=False, verbose=True):
        # num_batches = math.ceil(len(matches)/self.batch_size)
        for i in range(0, len(matches), self.batch_size):
            batch = self._get_batch(i, matches)
            self._extract_json_to_sql(conn, batch, first_entry, verbose)

    def _get_batch(self, i, matches):
        return matches[i:(i + self.batch_size)]

    def clean_table(self, conn):
        """
        delete all records from a given table in a given database
        """
        cur = conn.cursor()
        cur.execute(f'DELETE FROM {self.table}')

    def count_unique_records(self, conn):
        """
        checks the number of unique records in a given table
        """
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {self.table}")
        print(f"After insert, the table contains {cur.fetchall()[0][0]} records.")

    def fields_num(self):
        return len(self.paths)


class FromListInserter(Inserter):

    def __init__(self, table, path_to_list, paths=[], prefix="", pk_sql=[], batch_size=60000):
        super().__init__(table, paths, prefix, pk_sql, batch_size)
        if not path_to_list:
            raise ValueError
        self.path_to_list = path_to_list

    def _get_batch(self, i, matches):
        results = []
        for entry in matches:
            if get_value(entry, self.path_to_list) is not None:
                for element in get_value(entry, self.path_to_list):
                    results.append(self._get_list_element_with_id(element, entry))
        return results

    def _get_list_element_with_id(self, element, entry):
        return element


class FromListWithExternalIdInserter(FromListInserter):

    def __init__(self, table, path_to_list, path_to_id_outside_list, paths=[], prefix="", pk_sql=[], batch_size=60000):
        super().__init__(table, path_to_list, paths, prefix, pk_sql, batch_size)
        if not path_to_id_outside_list:
            raise ValueError
        self.path_to_id_outside_list = path_to_id_outside_list

    def _get_list_element_with_id(self, element, entry):
        return get_value(entry, self.path_to_id_outside_list), element

    def fields_num(self):
        return len(self.paths) + 1


class TypeInserter(Inserter):
    TABLE = 'TYPE'

    def __init__(self):
        super().__init__(TypeInserter.TABLE)

    def insert(self, conn, matches=None, first_entry=False, verbose=True):
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO {self.table} VALUES (1, 'Red'), (2, 'White'), (3, 'Sparkling'), (4, 'Rose'), (7, 'Dessert'), "
            f"(24, 'Fortified'), (25, 'Other')")


class WineryInserter(Inserter):
    TABLE = 'winery'
    PREFIX = 'vintage/wine/winery/'
    PATHS = ['id', 'name', 'seo_name', 'status']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(WineryInserter.TABLE,
                         paths=WineryInserter.PATHS,
                         prefix=WineryInserter.PREFIX,
                         pk_sql=WineryInserter.PK_SQL)


class CountryInserter(Inserter):
    TABLE = 'country'
    PREFIX = 'vintage/wine/region/country/'
    PATHS = ['code', 'name', 'native_name', 'seo_name', 'currency/code', 'regions_count',
             'users_count', 'wines_count', 'wineries_count']
    PK_SQL = ['code']

    def __init__(self):
        super().__init__(CountryInserter.TABLE,
                         paths=CountryInserter.PATHS,
                         prefix=CountryInserter.PREFIX,
                         pk_sql=CountryInserter.PK_SQL)


class RegionInserter(Inserter):
    TABLE = 'region'
    PREFIX = 'vintage/wine/region/'
    PATHS = ['id', 'name', 'name_en', 'seo_name', 'country/code']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(RegionInserter.TABLE,
                         paths=RegionInserter.PATHS,
                         prefix=RegionInserter.PREFIX,
                         pk_sql=RegionInserter.PK_SQL)


class StyleInserter(Inserter):
    TABLE = 'style'
    PREFIX = 'vintage/wine/style/'
    PATHS = ['id', 'seo_name', 'regional_name', 'varietal_name', 'name', 'description', 'blurb', 'body',
             'body_description', 'acidity', 'acidity_description', 'country/code', 'wine_type_id']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(StyleInserter.TABLE,
                         paths=StyleInserter.PATHS,
                         prefix=StyleInserter.PREFIX,
                         pk_sql=StyleInserter.PK_SQL)


class FoodInserter(Inserter):
    TABLE = 'food'
    PATHS = ['id', 'name', 'seo_name']
    PATH_TO_LIST = 'vintage/wine/style/food'
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(FoodInserter.TABLE,
                         paths=FoodInserter.PATHS,
                         path_to_list=FoodInserter.PATH_TO_LIST,
                         pk_sql=FoodInserter.PK_SQL,
                         from_list=True)


class FactInserter(Inserter):
    TABLE = 'facts'
    PATH_TO_LIST = 'vintage/wine/style/interesting_facts'
    PATH_TO_ID_OUTSIDE_LIST = 'vintage/wine/style/id'

    def __init__(self):
        super().__init__(FactInserter.TABLE,
                         path_to_list=FactInserter.PATH_TO_LIST,
                         path_to_id_outside_list=FactInserter.PATH_TO_ID_OUTSIDE_LIST,
                         from_list=True)

    def fields_num(self):
        return 2


class StyleFoodInserter(Inserter):
    TABLE = 'style_food'
    PATH_TO_LIST = 'vintage/wine/style/food'
    PATHS = ['id']
    PATH_TO_ID_OUTSIDE_LIST = 'vintage/wine/style/id'
    PK_SQL = ['style_id', 'food_id']

    def __init__(self):
        super().__init__(StyleFoodInserter.TABLE,
                         paths=StyleFoodInserter.PATHS,
                         path_to_list=StyleFoodInserter.PATH_TO_LIST,
                         path_to_id_outside_list=StyleFoodInserter.PATH_TO_ID_OUTSIDE_LIST,
                         pk_sql=StyleFoodInserter.PK_SQL,
                         from_list=True)


class GrapeInserter(Inserter):
    TABLE = 'grape'
    PATH_TO_LIST = 'vintage/wine/style/grapes'
    PATHS = ['id', 'name', 'seo_name', 'has_detailed_info', 'wines_count']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(GrapeInserter.TABLE,
                         paths=GrapeInserter.PATHS,
                         path_to_list=GrapeInserter.PATH_TO_LIST,
                         pk_sql=GrapeInserter.PK_SQL,
                         from_list=True)


class StyleGrapeInserter(Inserter):
    TABLE = 'style_grape'
    PATH_TO_LIST = 'vintage/wine/style/grapes'
    PATHS = ['id']
    PATH_TO_ID_OUTSIDE_LIST = 'vintage/wine/style/id'
    PK_SQL = ['style_id', 'grape_id']

    def __init__(self):
        super().__init__(StyleGrapeInserter.TABLE,
                         paths=StyleGrapeInserter.PATHS,
                         path_to_list=StyleGrapeInserter.PATH_TO_LIST,
                         path_to_id_outside_list=StyleGrapeInserter.PATH_TO_ID_OUTSIDE_LIST,
                         pk_sql=StyleGrapeInserter.PK_SQL,
                         from_list=True)


class CountryGrapeInserter(Inserter):
    TABLE = 'country_grape'
    PATH_TO_LIST = 'vintage/wine/style/grapes'
    PATHS = ['id']
    PATH_TO_ID_OUTSIDE_LIST = 'vintage/wine/style/country/code'
    PK_SQL = ['country_code', 'grape_id']

    def __init__(self):
        super().__init__(CountryGrapeInserter.TABLE,
                         paths=CountryGrapeInserter.PATHS,
                         path_to_list=CountryGrapeInserter.PATH_TO_LIST,
                         path_to_id_outside_list=CountryGrapeInserter.PATH_TO_ID_OUTSIDE_LIST,
                         pk_sql=CountryGrapeInserter.PK_SQL,
                         from_list=True)


class WineInserter(Inserter):
    TABLE = 'wine'
    PREFIX = 'vintage/wine/'
    PATHS = ['id', 'name', 'seo_name', 'type_id', 'vintage_type', 'is_natural',
             'region/id', 'winery/id', 'taste/structure/acidity', 'taste/structure/fizziness',
             'taste/structure/intensity', 'taste/structure/sweetness', 'taste/structure/tannin',
             'taste/structure/user_structure_count', 'taste/structure/calculated_structure_count',
             'style/id', 'statistics/status', 'statistics/ratings_count', 'statistics/ratings_average',
             'statistics/labels_count', 'statistics/vintages_count', 'has_valid_ratings']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(WineInserter.TABLE,
                         paths=WineInserter.PATHS,
                         prefix=WineInserter.PREFIX,
                         pk_sql=WineInserter.PK_SQL)


class PriceInserter(Inserter):
    TABLE = 'price'
    PREFIX = 'price/'
    PATHS = ['id', 'amount', 'discounted_from', 'type', 'visibility', 'currency/code', 'bottle_type/name']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(PriceInserter.TABLE,
                         paths=PriceInserter.PATHS,
                         prefix=PriceInserter.PREFIX,
                         pk_sql=PriceInserter.PK_SQL)


class VintageInserter(Inserter):
    TABLE = 'vintage'
    PREFIX = 'vintage/'
    PATHS = ['id', 'seo_name', 'name', 'wine/id', 'year', 'has_valid_ratings',
             'statistics/status', 'statistics/ratings_count', 'statistics/ratings_average',
             'statistics/labels_count', 'price/id']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(VintageInserter.TABLE,
                         paths=VintageInserter.PATHS,
                         prefix=VintageInserter.PREFIX,
                         pk_sql=VintageInserter.PK_SQL)


class ToplistInserter(Inserter):
    TABLE = 'toplist'
    PATH_TO_LIST = 'vintage/top_list_rankings'
    PATHS = ['top_list/id', 'top_list/location', 'top_list/name', 'top_list/seo_name', 'top_list/type', 'top_list/year']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(ToplistInserter.TABLE,
                         paths=ToplistInserter.PATHS,
                         path_to_list=ToplistInserter.PATH_TO_LIST,
                         pk_sql=ToplistInserter.PK_SQL,
                         from_list=True)


class VintageToplistInserter(Inserter):
    TABLE = 'vintage_toplist'
    PATH_TO_LIST = 'vintage/top_list_rankings'
    PATHS = ['top_list/id', 'top_list/rank', 'top_list/previous_rank', 'top_list/description']
    PATH_TO_ID_OUTSIDE_LIST = 'vintage/id'
    PK_SQL = ['vintage_id', 'toplist_id']

    def __init__(self):
        super().__init__(VintageToplistInserter.TABLE,
                         paths=VintageToplistInserter.PATHS,
                         path_to_list=VintageToplistInserter.PATH_TO_LIST,
                         pk_sql=VintageToplistInserter.PK_SQL,
                         path_to_id_outside_list=VintageToplistInserter.PATH_TO_ID_OUTSIDE_LIST,
                         from_list=True)


class UserInserter(Inserter):
    TABLE = 'user'
    PREFIX = 'user/'
    PATHS = ['id', 'seo_name', 'alias', 'is_featured', 'visibility', 'statistics/followers_count',
             'statistics/followings_count', 'statistics/ratings_count', 'statistics/ratings_sum',
             'statistics/reviews_count']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(UserInserter.TABLE,
                         paths=UserInserter.PATHS,
                         prefix=UserInserter.PREFIX,
                         pk_sql=UserInserter.PK_SQL)


class ActivityInserter(Inserter):
    TABLE = 'activity'
    PREFIX = 'activity/'
    PATHS = ['id', 'statistics/likes_count', 'statistics/comments_count']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(ActivityInserter.TABLE,
                         paths=ActivityInserter.PATHS,
                         prefix=ActivityInserter.PREFIX,
                         pk_sql=ActivityInserter.PK_SQL)


class ReviewInserter(Inserter):
    TABLE = 'review'
    PATHS = ['id', 'rating', 'note', 'language', 'created_at', 'aggregated', 'user/id', 'activity/id', 'tagged_note']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(ReviewInserter.TABLE,
                         paths=ReviewInserter.PATHS,
                         pk_sql=ReviewInserter.PK_SQL)


class VintageReviewInserter(Inserter):
    TABLE = 'vintage_review'
    PATHS = ['vintage/id', 'id']
    PK_SQL = ['vintage_id', 'review_id']

    def __init__(self):
        super().__init__(VintageReviewInserter.TABLE,
                         paths=VintageReviewInserter.PATHS,
                         pk_sql=VintageReviewInserter.PK_SQL)


def int_to_float(smth):
    """
    converts integers to floats
    """
    if isinstance(smth, int):
        return float(smth)
    elif smth == 'N.V.':
        return 0.0  # meaning, wine is of type 'non-vintage' and is made of grapes from more than one harvest
    else:
        return smth


def get_value(match_entry, path0):
    """
    Function that returns a value found at a given path inside a given JSON record
    """
    if path0 is None:
        current_el = match_entry
    else:
        path = path0.split('/')
        current_el = match_entry
        for p in path:
            if current_el is None:
                break
            current_el = current_el.get(p)
    return current_el
