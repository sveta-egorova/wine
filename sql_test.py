import pickle
import time
import mariadb


import sys
sys.path.append('..')
import settings


def connect_to_vivino_db():
    """
    connect to vivino db and return a connection instance
    """
    try:
        conn =  mariadb.connect(
                user="admin",
                password=settings.db_pass,
                host=settings.db_url,
                port=3306,
                database="vivino")
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    return conn


def insert_to_wineries(conn, matches, first_entry=True):
    """
    inserts data to correct fields in the wine table
    """
    table = 'winery'
    main = 'vintage/wine/winery/'
    paths = [main + 'id', main + 'name', main + 'seo_name', main + 'status']
    pk_sql = ['id']
    extract_json_to_sql(conn, matches, table, paths, pk_sql, first_entry)

def extract_json_to_sql(conn, matches_list, table_name, paths, pk_sql=[], first_entry=True):
    """
    Function that accepts the folowing arguments:
    * matches list: JSON list with data,
    * table_name: name of SQL table to include data,
    * paths: the paths leading to data in JSON corresponding to each column in SQL table,
    * pk_sql: the names of primary key columns in SQL (to check for uniqueness condition),
    * conn: active connection to a database
    * first_entry: boolean indicating whether it's the first time data is written to the table

    Function inserts data and (for the first entry) checks whether the resulting number of unique records in SQL
    matches the number of unique records in JSON.

    """
    cur = conn.cursor()

    timepoint_1 = time.time()

    all_args = {}

    for entry in matches_list:
        values_entry = [get_value(entry, path) for path in paths]
        pk_values = values_entry[:len(pk_sql)]
        if all(pk_value is not None for pk_value in pk_values):
            all_args[tuple(pk_values)] = tuple(values_entry)

    if len(pk_sql) == 0:
        if_duplicates_do_nothing = ""
    if len(pk_sql) == 1:
        if_duplicates_do_nothing = f" ON DUPLICATE KEY UPDATE {pk_sql[0]} = {pk_sql[0]}"
    if len(pk_sql) == 2:
        if_duplicates_do_nothing = f" ON DUPLICATE KEY UPDATE {pk_sql[0]} = {pk_sql[0]}, {pk_sql[1]} = {pk_sql[1]}"

    query = f"""
        INSERT INTO {table_name} VALUES ({', '.join('?' * len(paths))})
        {if_duplicates_do_nothing};
    """.strip()
    cur.executemany(query, list(all_args.values()))
    conn.commit()

    timepoint_2 = time.time()
    print('Insertion complete and took {} s.'.format(timepoint_2 - timepoint_1))

    if first_entry:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM {};'.format(table_name))
        unique_sql = cur.fetchall()[0][0]
        unique_python = len(list(all_args.values()))
        if unique_python == unique_sql:
            print('Number of unique records is accurate')
        else:
            print('Something went wrong')

def get_value(match_entry, path0):
    """
    Function that returns a value found at a given path inside a given JSON record
    """
    path = path0.split('/')
    current_el = match_entry
    for p in path:
        if current_el is None:
            break
        current_el = current_el.get(p)
    return current_el


def remove_wine_duplicates(json_data):
    """
    get distinct list from JSON
    :param json_data:
    :return:
    """
    distinct_dict = {entry['vintage']['id']: entry for entry in json_data}
    recovered_data_distinct = distinct_dict.values()
    return list(recovered_data_distinct)

def get_dataset(path):
    """
    get the list from a file
    :param f:
    :return:
    """
    with open(path, 'rb') as f:
        recovered_data = pickle.load(f)
    return remove_wine_duplicates(recovered_data)


if __name__ == "__main__":
    matches = get_dataset("backup_data/full_match_list")
    conn = connect_to_vivino_db()
    try:
        insert_to_wineries(conn, matches, True)
    finally:
        conn.close()
