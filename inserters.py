from abc import ABC
from typing import List, Dict
import time
from ratelimiter import RateLimiter


class Inserter(ABC):

    def __init__(self, table: str, prefix="", paths=[], pk_sql=[], batch_size=60000):
        self.table = table
        self.prefix = prefix
        self.paths = [prefix + path for path in paths]
        self.pk_sql = pk_sql
        self.batch_size = batch_size

    @staticmethod
    def _get_value(match_entry: Dict, path0: str) -> any:
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

    @staticmethod
    def _format_numbers(smth: any) -> any:
        """
        Function that converts integers to floats, and non-vintage year mark ('N.V') to zeroes
        """
        if isinstance(smth, int):
            return float(smth)
        elif smth == 'N.V.':
            return 0.0  # meaning, wine is of type 'non-vintage' and is made of grapes from more than one harvest
        else:
            return smth

    def _extract_args(self, matches_list: List[Dict]) -> List[tuple]:
        """
        Function that converts raw JSON data to a list of tuple with specific fields necessary for a given inserter,
        no duplicates and no missing primary keys
        """
        all_args = {}
        for entry in matches_list:
            # here, each entry represents a Python dictionary
            values_entry = [Inserter._format_numbers(Inserter._get_value(entry, path)) for path in self.paths]
            pk_values = values_entry[:len(self.pk_sql)]  # provided primary keys always come first
            #  We'd like to eliminate duplicates from our batch, as well as records with Null primary keys.
            if all(pk_value is not None for pk_value in pk_values):
                all_args[tuple(pk_values)] = tuple(values_entry)
        return list(all_args.values())

    def _fields_num(self):
        """
        Function that returns the number of fields in SQL for a given inserter
        """
        return len(self.paths)

    # @RateLimiter(1, 60)
    def _insert_json_to_sql(self, conn, matches: List[Dict], verbose: bool) -> None:
        """
        Function inserts JSON data to SQL and (if it's the first entry) checks whether the resulting number of unique
        records in SQL matches the number of unique records in JSON.
        """
        cur = conn.cursor()
        timepoint_1 = time.time()
        args = self._extract_args(matches)

        if verbose:
            # find the number of records before insertion
            cur2 = conn.cursor()
            cur2.execute('SELECT COUNT(*) FROM {}'.format(self.table))
            records_sql_before = cur2.fetchall()[0][0]  # counts unique entries in the table before the insert statement
            cur2.close()

        # part of the query that tells to do nothing on duplicate keys if such entry already exists,
        # depending on the number of primary keys
        if len(self.pk_sql) == 0:
            if_duplicates_do_nothing = ''
        else:
            if_duplicates_do_nothing = ' ON DUPLICATE KEY UPDATE ' + \
                                       ', '.join([f'{key} = {key}' for key in self.pk_sql])

        query = f"""
                INSERT INTO {self.table} VALUES ({', '.join('?' * self._fields_num())})
                {if_duplicates_do_nothing}
            """.strip()
        timepoint_2 = time.time()
        print(f'Up until execution the current iteration took {round(timepoint_2 - timepoint_1, 2)} s.')
        cur.executemany(query, args)
        timepoint_3 = time.time()
        print(f'Program took {round(timepoint_3 - timepoint_2, 2)} s. to execute')
        conn.commit()
        timepoint_4 = time.time()
        print(f'Program took {round(timepoint_4 - timepoint_3, 2)} s. to commit changes')

        if verbose:
            print(f'Insertion to {self.table} complete and took {round(time.time() - timepoint_1, 2)} s.')

            cur3 = conn.cursor()
            cur3.execute('SELECT COUNT(*) FROM {}'.format(self.table))
            records_sql_after = cur3.fetchall()[0][0]  # counts unique entries in the table after the insert statement
            args_python = len(args)  # counts unique entries in JSON
            if args_python == (records_sql_after - records_sql_before):
                print(f'Number of unique records in table {self.table} is accurate and equals {records_sql_after} '
                      f'after insert')
            else:
                print(f'The table {self.table} increased by {(records_sql_after - records_sql_before)} records')

    def _get_batch(self, i: int, matches: List[Dict]) -> List[Dict]:
        """
        Function that returns a batch to be inserted to SQL (controls the weight)
        """
        return matches[i:(i + self.batch_size)]

    def insert(self, conn, matches: List[Dict], verbose: bool) -> None:
        """
        Function that inserts JSON data into SQL (splitting into batches along the way)
        :param conn: active connection to x
        :param matches: original JSON
        :param verbose: boolean if asked to print the progress of loading
        :return: None
        """
        for i in range(0, len(matches), self.batch_size):
            batch = self._get_batch(i, matches)
            self._insert_json_to_sql(conn, batch, verbose)

    def clean_table(self, conn) -> None:
        """
        Function that deletes all records from a given table in a given database
        """
        cur = conn.cursor()
        cur.execute(f'DELETE FROM {self.table}')
        conn.commit()

    def count_records(self, conn, when='After insert') -> None:
        """
        Function that checks the number of unique records in a given table
        """
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {self.table}")
        print(f"{when}, the table {self.table} contains {cur.fetchall()[0][0]} records.")


class FromListInserter(Inserter):

    def __init__(self, table, path_to_list, paths_from_list=[], pk_sql=[], batch_size=60000):
        super().__init__(table, '', paths_from_list, pk_sql, batch_size)
        if not path_to_list:
            raise ValueError
        self.path_to_list = path_to_list

    def _get_list_element_with_id(self, element: any, entry: Dict) -> any:
        return element

    def _get_batch(self, i: int, matches: List[Dict]) -> List[Dict]:
        results = []
        for entry in matches: # here, entry is a dictionary that contains required values
            if Inserter._get_value(entry, self.path_to_list) is not None:
                for element in Inserter._get_value(entry, self.path_to_list):
                    results.append(self._get_list_element_with_id(element, entry))
        return results


class FromListWithExternalIdInserter(FromListInserter):

    def __init__(self, table, path_to_list, path_to_id_outside_list, paths_from_list=[], pk_sql=[], batch_size=60000):
        super().__init__(table, path_to_list, paths_from_list, pk_sql, batch_size)
        if not path_to_id_outside_list:
            raise ValueError
        self.path_to_id_outside_list = path_to_id_outside_list

    def _get_list_element_with_id(self, element: any, entry: Dict): #TODO type annot
        return Inserter._get_value(entry, self.path_to_id_outside_list), element

    def _extract_args(self, matches_list: List) -> List[tuple]: #TODO type annot
        all_args = {}
        for entry in matches_list:
            # here, each entry represents a tuple with two elements:
            # id and a JSON element for extraction of necessary paths
            if len(self.paths) == 0:
                # for record in entry[1]:
                all_args[entry] = entry
            else:
                values_entry = [entry[0]] + \
                               [Inserter._format_numbers(Inserter._get_value(entry[1], path)) for path in self.paths]
                pk_values = values_entry[:len(self.pk_sql)]  # provided primary keys always come first
                #  We'd like to eliminate duplicates from our batch, as well as records with Null primary keys.
                if all(pk_value is not None for pk_value in pk_values):
                    all_args[tuple(pk_values)] = tuple(values_entry)
        return list(all_args.values())

    def _fields_num(self):
        return len(self.paths) + 1


class TypeInserter(Inserter):
    TABLE = 'type'

    def __init__(self):
        super().__init__(TypeInserter.TABLE)

    def insert(self, conn, matches=None, verbose=True):
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO {self.table} VALUES (1, 'Red'), (2, 'White'), (3, 'Sparkling'), (4, 'Rose'), (7, 'Dessert'), "
            f"(24, 'Fortified'), (25, 'Other') ON DUPLICATE KEY UPDATE id = id")
        conn.commit()


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


class FoodInserter(FromListInserter):
    TABLE = 'food'
    PATHS = ['id', 'name', 'seo_name']
    PATH_TO_LIST = 'vintage/wine/style/food'
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(FoodInserter.TABLE,
                         path_to_list=FoodInserter.PATH_TO_LIST,
                         paths_from_list=FoodInserter.PATHS,
                         pk_sql=FoodInserter.PK_SQL)


class FactInserter(FromListWithExternalIdInserter):
    TABLE = 'facts'
    PATH_TO_LIST = 'vintage/wine/style/interesting_facts'
    PATH_TO_ID_OUTSIDE_LIST = 'vintage/wine/style/id'

    def __init__(self):
        super().__init__(FactInserter.TABLE,
                         path_to_list=FactInserter.PATH_TO_LIST,
                         path_to_id_outside_list=FactInserter.PATH_TO_ID_OUTSIDE_LIST)

    def _fields_num(self):
        return 2


class StyleFoodInserter(FromListWithExternalIdInserter):
    TABLE = 'style_food'
    PATH_TO_LIST = 'vintage/wine/style/food'
    PATHS = ['id']
    PATH_TO_ID_OUTSIDE_LIST = 'vintage/wine/style/id'
    PK_SQL = ['style_id', 'food_id']

    def __init__(self):
        super().__init__(StyleFoodInserter.TABLE,
                         path_to_list=StyleFoodInserter.PATH_TO_LIST,
                         paths_from_list=StyleFoodInserter.PATHS,
                         path_to_id_outside_list=StyleFoodInserter.PATH_TO_ID_OUTSIDE_LIST,
                         pk_sql=StyleFoodInserter.PK_SQL)


class GrapeInserter(FromListInserter):
    TABLE = 'grape'
    PATH_TO_LIST = 'vintage/wine/style/grapes'
    PATHS = ['id', 'name', 'seo_name', 'has_detailed_info', 'wines_count']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(GrapeInserter.TABLE,
                         path_to_list=GrapeInserter.PATH_TO_LIST,
                         paths_from_list=GrapeInserter.PATHS,
                         pk_sql=GrapeInserter.PK_SQL)


class StyleGrapeInserter(FromListWithExternalIdInserter):
    TABLE = 'style_grape'
    PATH_TO_LIST = 'vintage/wine/style/grapes'
    PATHS = ['id']
    PATH_TO_ID_OUTSIDE_LIST = 'vintage/wine/style/id'
    PK_SQL = ['style_id', 'grape_id']

    def __init__(self):
        super().__init__(StyleGrapeInserter.TABLE,
                         path_to_list=StyleGrapeInserter.PATH_TO_LIST,
                         paths_from_list=StyleGrapeInserter.PATHS,
                         path_to_id_outside_list=StyleGrapeInserter.PATH_TO_ID_OUTSIDE_LIST,
                         pk_sql=StyleGrapeInserter.PK_SQL)


class CountryGrapeInserter(FromListWithExternalIdInserter):
    TABLE = 'country_grape'
    PATH_TO_LIST = 'vintage/wine/style/grapes'
    PATHS = ['id']
    PATH_TO_ID_OUTSIDE_LIST = 'vintage/wine/style/country/code'
    PK_SQL = ['country_code', 'grape_id']

    def __init__(self):
        super().__init__(CountryGrapeInserter.TABLE,
                         path_to_list=CountryGrapeInserter.PATH_TO_LIST,
                         paths_from_list=CountryGrapeInserter.PATHS,
                         path_to_id_outside_list=CountryGrapeInserter.PATH_TO_ID_OUTSIDE_LIST,
                         pk_sql=CountryGrapeInserter.PK_SQL)


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
    PREFIX = ''
    PATHS = ['vintage/id', 'vintage/seo_name', 'vintage/name', 'vintage/wine/id', 'vintage/year',
             'vintage/has_valid_ratings', 'vintage/statistics/status', 'vintage/statistics/ratings_count',
             'vintage/statistics/ratings_average', 'vintage/statistics/labels_count', 'price/id', 'price/amount']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(VintageInserter.TABLE,
                         paths=VintageInserter.PATHS,
                         prefix=VintageInserter.PREFIX,
                         pk_sql=VintageInserter.PK_SQL)


class ToplistInserter(FromListInserter):
    TABLE = 'toplist'
    PATH_TO_LIST = 'vintage/top_list_rankings'
    PATHS = ['top_list/id', 'top_list/location', 'top_list/name', 'top_list/seo_name', 'top_list/type', 'top_list/year']
    PK_SQL = ['id']

    def __init__(self):
        super().__init__(ToplistInserter.TABLE,
                         path_to_list=ToplistInserter.PATH_TO_LIST,
                         paths_from_list=ToplistInserter.PATHS,
                         pk_sql=ToplistInserter.PK_SQL)


class VintageToplistInserter(FromListWithExternalIdInserter):
    TABLE = 'vintage_toplist'
    PATH_TO_LIST = 'vintage/top_list_rankings'
    PATHS = ['top_list/id', 'rank', 'previous_rank', 'description']
    PATH_TO_ID_OUTSIDE_LIST = 'vintage/id'
    PK_SQL = ['vintage_id', 'toplist_id']

    def __init__(self):
        super().__init__(VintageToplistInserter.TABLE,
                         path_to_list=VintageToplistInserter.PATH_TO_LIST,
                         paths_from_list=VintageToplistInserter.PATHS,
                         pk_sql=VintageToplistInserter.PK_SQL,
                         path_to_id_outside_list=VintageToplistInserter.PATH_TO_ID_OUTSIDE_LIST)


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
                         pk_sql=ReviewInserter.PK_SQL,
                         batch_size=6000)


class VintageReviewInserter(Inserter):
    TABLE = 'vintage_review'
    PATHS = ['vintage/id', 'id']
    PK_SQL = ['vintage_id', 'review_id']

    def __init__(self):
        super().__init__(VintageReviewInserter.TABLE,
                         paths=VintageReviewInserter.PATHS,
                         pk_sql=VintageReviewInserter.PK_SQL)



