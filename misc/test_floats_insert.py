import mariadb
import settings


if __name__ == '__main__':
    conn = mariadb.connect(
        user="admin",
        password=settings.db_pass,
        host=settings.db_url,
        port=3306,
        database="vivino")

    cur = conn.cursor()
    try:
        cur.executemany('INSERT INTO test_floats(num_int) VALUES (?)', [(0.51,), (2.4999,)])
        conn.commit()
    finally:
        conn.close()
