from abc import ABC
import sys, os
from typing import List

import mariadb
import settings
import pickle
import time


class Inserter(ABC):

    def __init__(self, table, prefix, paths, pk_sql, batch_size=60000):
        self.table = table
        self.prefix = prefix
        self.paths = [prefix + path for path in paths]
        self.pk_sql = pk_sql
        self.batch_size = batch_size

    def _extract_json_to_sql(self, conn, matches_list, first_entry, from_list, verbose):
        """
        Function that accepts the folowing arguments:
        * conn: active connection to a database
        * matches list: JSON list with data, 
        * table_name: name of SQL table to include data,  
        * paths: the paths leading to data in JSON corresponding to each column in SQL table,
        * pk_sql: the names of primary key columns in SQL (to check for uniqueness condition),
        * first_entry: boolean indicating whether it's the first time data is written to the table

        Function inserts data and (for the first entry) checks whether the resulting number of unique records in SQL
        matches the number of unique records in JSON.

        """
        cur = conn.cursor()
        timepoint_1 = time.time()
        all_args = {}

        for entry in matches_list:
            if from_list:
                # if data is passed from json list, entry contains a tuple (id, relevant_data), which need to be flattened in a single arg list
                values_entry = [entry[0]] + [int_to_float(get_value(entry[1], path)) for path in self.paths]
                if len(self.paths) == 0:
                    for record in entry[1:]:
                        all_args[(entry[0], record)] = (entry[0], record)
            else:
                values_entry = [int_to_float(get_value(entry, path)) for path in self.paths]
            pk_values = values_entry[:len(self.pk_sql)]

            if all(pk_value is not None for pk_value in pk_values) and len(self.paths) > 0:
                all_args[tuple(pk_values)] = tuple(values_entry)

        if len(self.pk_sql) == 1:
            if_duplicates_do_nothing = f" ON DUPLICATE KEY UPDATE {self.pk_sql[0]} = {self.pk_sql[0]}"
        elif len(self.pk_sql) == 2:
            if_duplicates_do_nothing = f" ON DUPLICATE KEY UPDATE {self.pk_sql[0]} = {self.pk_sql[0]}, " \
                                       f"{self.pk_sql[1]} = {self.pk_sql[1]}"
        else:
            if_duplicates_do_nothing = ""

        fields_num = len(self.paths)
        if from_list and len(self.paths) > 0:
            fields_num += 1
        elif len(self.paths) == 0:
            fields_num = 2

        query = f"""
                INSERT INTO {self.table} VALUES ({', '.join('?' * fields_num)})
                {if_duplicates_do_nothing};
            """.strip()
        cur.executemany(query, list(all_args.values()))
        conn.commit()

        timepoint_2 = time.time()
        if verbose:
            print('Insertion complete and took {} s.'.format(timepoint_2 - timepoint_1))

        if first_entry:
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) FROM {};'.format(self.table))
            unique_sql = cur.fetchall()[0][0]
            unique_python = len(list(all_args.values()))
            if unique_python == unique_sql:
                print('Number of unique records is accurate')
            else:
                print('Something went wrong')


    def _extract_json_list_to_sql(self, conn, matches_list, path_to_list, paths_from_list, path_to_id_outside_list,
                                  first_entry, verbose):
        """
        converts list stored inside json to a normal list
        """
        results = []
        for entry in matches_list:
            if get_value(entry, path_to_list) is not None:
                for element in get_value(entry, path_to_list):
                    if path_to_id_outside_list == "":
                        results.append(element)
                    else:
                        results.append((get_value(entry, path_to_id_outside_list), element))
        if path_to_id_outside_list == "":
            from_json_list_with_id = False
        else:
            from_json_list_with_id = True
        self._extract_json_to_sql(conn, results, first_entry, from_list, verbose)


    def insert(self, conn, matches, first_entry=False, verbose=False):
        for i in range(0, len(matches), self.batch_size):
            batch = matches[i:(i + self.batch_size)]
            self._extract_json_to_sql(conn, batch, first_entry, verbose)


class UserInserter(Inserter):

    PATHS = ['id', 'seo_name', 'alias']  # TODO ...

    def __init__(self, batch_size):
        super().__init__('user', 'user/', UserInserter.PATHS, ['id'], batch_size)


class WineInserter(Inserter):

    PATHS = ['id', 'name', 'seo_name']  # TODO ...

    def __init__(self, batch_size):
        super().__init__('wine', 'vintage/wine/', WineInserter.PATHS, ['id'], batch_size)


def connect_to_vivino_db():
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
        sys.exit(1)
    return conn


def read_files_insert_to_sql(dir: str, inserters: List[Inserter], batch_size, first_entry, verbose: bool):
    for filename in os.listdir(dir):
        if filename != ".DS_Store":
            with open(f'{dir}/{filename}', 'rb') as f:  #TODO startswith instead of !=
                cur_data = pickle.load(f)
                conn = connect_to_vivino_db()
                try:
                    for inserter in inserters:
                        inserter.insert(conn, cur_data, first_entry, verbose)
                    # insert_by_batch(conn, cur_reviews, False, False)
                finally:
                    conn.close()

                print(f"{len(cur_data)} records loaded to database from file {filename}...")
    print("Loading complete.")


def int_to_float(smth):
    """
    converts integers to floats
    """
    if isinstance(smth, int):
        return float(smth)
    elif smth == 'N.V.':
        return 0.0    # meaning, wine is of type 'non-vintage' and is made of grapes from more than one harvest
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



if __name__ == '__main__':

    base_dir = 'backup_data/'
    batch_size = 60000
    first_entry = True
    verbose = True

    mapping = {
        'explore': [TypeInserter(), WineryInserter(), CountryInserter(), RegionInserter(), StyleInserter(),
                    FoodInserter(), FactInserter(), StyleFoodInserter(), GrapeInserter(), StyleGrapeInserter(),
                    CountryGrapeInserter(), WineInserter(), PriceInserter(), VintageInserter(), ToplistInserter(),
                    VintageToplistInserter()],
        'reviews': [UserInserter(), ActivityInserter(), ReviewInserter(), VintageReviewInserter()]
    }

    for directory, inserters in mapping:
        # for inserter in inserters:
        read_files_insert_to_sql(base_dir + directory, inserters, #batch_size, first_entry, verbose)

