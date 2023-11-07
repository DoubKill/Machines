import os
import sqlite3
import psycopg2


class SQLiteConnector(object):

    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()

    def get_columns(self, table_name):
        col_cursor = self.cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [i[1] for i in col_cursor.fetchall()]
        return columns

    def insert(self, table_name, data):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in range(len(data))])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = tuple(data.values())
        self.cursor.execute(query, values)
        self.conn.commit()

    def update(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()

    # 使用executemany批量更新数据
    def multi_update(self, sql, data):
        self.cursor.executemany(sql, data)
        self.conn.commit()

    def add_column(self, table_name, column_name, data_type):
        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type}"
        self.cursor.execute(query)
        self.conn.commit()

    def run(self, sql, table_name='stock_inventory'):
        columns = self.get_columns(table_name)
        ex_cursor = self.cursor.execute(sql)
        rows = ex_cursor.fetchall()
        dict_data = [dict(zip(columns, row)) for row in rows]
        return dict_data


class PostgresConnector(object):

    def __init__(self):
        self.ip = os.environ.get('POSTGRES_IP', '10.10.130.58')
        self.port = os.environ.get('POSTGRES_PORT', 5432)
        self.db_name = os.environ.get('POSTGRES_DB_NAME', 'dev_lbw2')
        self.user = os.environ.get('POSTGRES_USER', 'postgres')
        self.password = os.environ.get('POSTGRES_PASSWORD', 'gz@admin')
        self.conn = psycopg2.connect(host=self.ip, port=self.port, dbname=self.db_name, user=self.user, password=self.password, connect_timeout=1)
        self.cursor = self.conn.cursor()

    def run(self, sql):
        self.cursor.execute(sql)
        column_names = [desc[0] for desc in self.cursor.description]
        rows = self.cursor.fetchall()
        dict_data = [dict(zip(column_names, row)) for row in rows]
        return dict_data
