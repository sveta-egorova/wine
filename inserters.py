from abc import ABC


class Inserter(ABC):

    def __init__(self, table, prefix, paths, pk_sql, batch_size):
        self.table = table
        self.prefix = prefix
        self.paths = [prefix + path for path in paths]
        self.pk_sql = pk_sql
        self.batch_size = batch_size

    def _extract_json_to_sql(self, conn, matches):
        # absolutely the same code
        pass

    def insert(self, conn, matches, first_entry=False, verbose=False):
        for i in range(0, len(matches), self.batch_size):
            batch = matches[i:(i + self.batch_size)]
            self._extract_json_to_sql(conn, batch)


class UserInserter(Inserter):

    PATHS = ['id', 'seo_name', 'alias']  # TODO ...

    def __init__(self, batch_size):
        super().__init__('user', 'user/', UserInserter.PATHS, ['id'], batch_size)


class WineInserter(Inserter):

    PATHS = ['id', 'name', 'seo_name']  # TODO ...

    def __init__(self, batch_size):
        super().__init__('wine', 'vintage/wine/', WineInserter.PATHS, ['id'], batch_size)


def read_files_insert_to_sql(dir, inserter):
    matches = []  # TODO read from somewhere
    conn = ...
    try:
        inserter.insert(conn, matches)
    finally:
        conn.close()


if __name__ == '__main__':

    base_dir = 'backup_data'
    mapping = {
        'reviews': [UserInserter(1000), ReviewInserter(1000), KeywordInserter(1000)],
        'explore': [WineInserter(1000), VintageInserter(1000)]
    }

    for directory, inserters in mapping:
        for inserter in inserters:
            read_files_insert_to_sql(base_dir + '/' + directory, inserter)



def download_wines(price_min=0, price_max=400):
    pass