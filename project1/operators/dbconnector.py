# import psycopg2
import mysql.connector
from psycopg2.pool import SimpleConnectionPool
from mysql.connector.pooling import MySQLConnectionPool


class DBConnection:
    def __init__(self, db_type='postgresql', mpl=-1):
        self.db_type = db_type
        self.mpl = mpl
        self.commit_counter = 0
        self.query_counter = 0
        if db_type == 'postgresql':
            # self.connection = psycopg2.connect(database='cs223', user='cs223', password='cs223')
            conn = "dbname='cs223' user='cs223' host='localhost' password='cs223'"
            # pool define with 10 live connections
            self.connection_pool = SimpleConnectionPool(minconn=1, maxconn=self.mpl, dsn=conn)
            self.connection = self.connection_pool.getconn()
            self.connection.autocommit = False
            # self.cursor = self.get_cursor_postgresql()
            # self.cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_mode}")
            # self.connection.commit()


        elif db_type == 'mysql':
            if mpl > 0:
                global pool
                pool = MySQLConnectionPool(user='cs223', password='cs223', pool_size=self.mpl)
                self.connection = pool.get_connection()
                # self.connection = mysql.connector.connect(user='cs223', password='cs223', pool_size=self.mpl)
            else:
                self.connection = mysql.connector.connect(user='cs223', password='cs223')
            self.get_cursor().execute("use cs223; ")
            # print(f"auto commit --- {self.connection.autocommit}")

    def get_cursor_postgresql(self):
        try:
            # yield self.connection.cursor()
            # self.connection.commit()
            yield self.connection
        finally:
            self.connection_pool.putconn(self.connection)

    def get_cursor(self):
        if self.db_type == 'postgresql':
            self.get_cursor_postgresql()
            # return self.connection.cursor()
        return self.connection.cursor()

    def close(self, cursor=None):
        # if self.db_type == 'mysql':
        #     self.connection_pool.closeall()
        #     return
        if cursor:
            cursor.close()
        self.connection.close()
        return

    def commit(self):
        self.commit_counter += 1
        self.connection.commit()
