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

def int_to_float(smth):
    if isinstance(smth, int):
        return float(smth)
    elif smth == 'N.V.':
        return 0    # meaning, wine is of type 'non-vintage' and is made of grapes from more than one harvest
    else:
        return smth

def extract_json_to_sql(conn, matches_list, table_name, paths, pk_sql=[], first_entry=True, from_json_list_with_id = False):
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
        if from_json_list_with_id:
            # if data is passed from json list, entry contains a tuple (id, relevant_data), which need to be flattened in a single arg list
            values_entry = [entry[0]] + [int_to_float(get_value(entry[1], path)) for path in paths]
            if paths == []:
                for record in entry[1:]:
                    all_args[(entry[0], record)] = (entry[0], record)
        else:
            values_entry = [int_to_float(get_value(entry, path)) for path in paths]
        pk_values = values_entry[:len(pk_sql)]
        if all(pk_value is not None for pk_value in pk_values) and paths != []:
            all_args[tuple(pk_values)] = tuple(values_entry)

    if len(pk_sql) == 0:
        if_duplicates_do_nothing = ""
    elif len(pk_sql) == 1:
        if_duplicates_do_nothing = f" ON DUPLICATE KEY UPDATE {pk_sql[0]} = {pk_sql[0]}"
    elif len(pk_sql) == 2:
        if_duplicates_do_nothing = f" ON DUPLICATE KEY UPDATE {pk_sql[0]} = {pk_sql[0]}, {pk_sql[1]} = {pk_sql[1]}"

    fields_num = len(paths)
    if from_json_list_with_id and paths != []:
        fields_num += 1
    elif paths == []:
        fields_num = 2

    query = f"""
        INSERT INTO {table_name} VALUES ({', '.join('?' * fields_num)})
        {if_duplicates_do_nothing};
    """.strip()
    try:
        cur.executemany(query, list(all_args.values()))
        conn.commit()
    except Exception as e:
        print('oops')

    timepoint_2 = time.time()
    print('Insertion complete and took {} s.'.format(timepoint_2 - timepoint_1))

    if first_entry:
        # cur.close()
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM {};'.format(table_name))
        unique_sql = cur.fetchall()[0][0]
        unique_python = len(list(all_args.values()))
        if unique_python == unique_sql:
            print('Number of unique records is accurate')
        else:
            print('Something went wrong')

def extract_json_list_to_sql(conn, matches_list, table_name, path_to_list, paths_from_list, \
                             path_to_id_outside_list="", pk_sql=[], first_entry=True):
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
    extract_json_to_sql(conn, results, table_name, paths_from_list, pk_sql, first_entry, from_json_list_with_id)


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
    return recovered_data

def clean_sql_table(conn, table_name):
    """
    delete all records from a given table in a given database
    """
    cur = conn.cursor()
    cur.execute(f'DELETE FROM {table_name}')

def insert_to_food(conn, matches, first_entry=True):
    """
    inserts data to correct fields in the style table
    """
    table = 'food'
    path_to_list = 'vintage/wine/style/food'
    paths_from_list = ['id', 'name', 'seo_name']
    pk_sql = ['id']
    extract_json_list_to_sql(conn, matches, table, path_to_list, paths_from_list, pk_sql, first_entry)

def insert_to_wine(conn, matches, first_entry=True):
    """
    inserts data to correct fields in the wine table
    """
    table = 'wine'
    main = 'vintage/wine/'
    paths = [main + 'id', main + 'name', main + 'seo_name', main + 'type_id', main + 'vintage_type', main + 'is_natural',\
             main + 'region/id', main + 'winery/id', main + 'taste/structure/acidity', main + 'taste/structure/fizziness',\
             main + 'taste/structure/intensity', main + 'taste/structure/sweetness', main + 'taste/structure/tannin',\
             main + 'taste/structure/user_structure_count', main + 'taste/structure/calculated_structure_count', \
             main + 'style/id', main + 'statistics/status', main + 'statistics/ratings_count', main + 'statistics/ratings_average',\
             main + 'statistics/labels_count', main + 'statistics/vintages_count', main + 'has_valid_ratings']
    pk_sql = ['id']
    extract_json_to_sql(conn, matches, table, paths, pk_sql, first_entry)


def insert_to_vintage(conn, matches, first_entry=True):
    """
    inserts data to correct fields in the vintage table
    """
    table = 'vintage'
    main = 'vintage/'
    paths = [main + 'id', main + 'seo_name', main + 'name', main + 'wine/id', main + 'year', main + 'has_valid_ratings',\
             main + 'statistics/status', main + 'statistics/ratings_count', main + 'statistics/ratings_average',\
             main + 'statistics/labels_count', 'price/id']
    pk_sql = ['id']
    extract_json_to_sql(conn, matches, table, paths, pk_sql, first_entry, False)


def insert_to_toplist(conn, matches, first_entry=True):
    """
    inserts data to correct fields in the toplist table
    """
    table = 'toplist'
    path_to_list = 'vintage/top_list_rankings'
    paths_from_list = ['id', 'location', 'name', 'seo_name', 'type', 'year']
    pk_sql = ['id']
    extract_json_list_to_sql(conn, matches, table, path_to_list, paths_from_list, pk_sql=pk_sql, first_entry=first_entry)


def insert_to_facts(conn, matches, first_entry=True):
    """
    inserts data to correct fields in the facts table
    """
    table = 'facts'
    path_to_list = 'vintage/wine/style/interesting_facts'
    paths_from_list = []
    path_to_id_outside_list = 'vintage/wine/style/id'
    pk_sql = []
    extract_json_list_to_sql(conn, matches, table, path_to_list, paths_from_list, path_to_id_outside_list, pk_sql, first_entry)


def batching(func, batch_size):
    """
    performs a function with a big list broken down into batches of certain siz
    """
    def do_smth_by_batches(conn, big_list, first_entry):
        num_batches = len(big_list) // batch_size
        for i in range(num_batches + 1):
            if i < num_batches + 1:
                cur_batch = big_list[i*batch_size : (i+1)*batch_size]
            elif i == num_batches + 1:
                cur_batch = big_list[- (len(big_list) - num_batches * batch_size):]
            func(conn, cur_batch, first_entry)
    return do_smth_by_batches


def insert_to_users(conn, matches, first_entry=True):
    """
    inserts data to correct fields in the user table
    """
    table = 'user'
    main = 'user/'
    paths = [main + 'id', main + 'seo_name', main + 'alias', main + 'is_featured', main + 'visibility', \
            main + 'statistics/followers_count', main + 'statistics/followings_count', main + 'statistics/ratings_count',\
            main + 'statistics/ratings_sum', main + 'statistics/reviews_count']
    pk_sql = ['id']
    extract_json_to_sql(conn, matches, table, paths, pk_sql, first_entry)

if __name__ == "__main__":
    reviews = get_dataset("backup_data/reviews/Italy_2017")
    conn = connect_to_vivino_db()

    # query = 'INSERT INTO user VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE id = id;'
    # arg1 = (10577601.0, 'davidlowe10', 'David Lowe', 0.0, 'all', 2.0, 0.0, 17.0, 66.5, 11.0)
    # arg2 = (6827219.0, 'luis-abb', 'Luis Abbate', 0.0, 'all', 13.0, 15.0, 286.0, 1184.0, 272.0)
    # cur = conn.cursor()
    #
    # try:
    #     cur.executemany(query, [arg1, arg2])
    #     conn.commit()
    # finally:
    #     conn.close()
    #
    # try:
    #     insert_to_users(conn, reviews, True)
    # finally:
    #     conn.close()

    insert_by_batch = batching(insert_to_users, 50000)
    try:
        clean_sql_table(conn, 'user')
        insert_by_batch(conn, reviews, False)
    finally:
        conn.close()

    # reviews_first_batch = reviews[:50000]
    # print(len(set([item['user']['id'] for item in reviews])))