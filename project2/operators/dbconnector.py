import psycopg2


class DBConnection:
    def __init__(self, id=-1):
        if id == -1:
            self.connection = psycopg2.connect(database='cs223', user='cs223', password='cs223')
        else:
            self.connection = psycopg2.connect(database=f'cs223_{id}', user='cs223', password='cs223')

    def get_cursor(self):
        return self.connection.cursor()

    def set_isolation_level(self, level):
        if level == 'auto':
            self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def close(self, cursor=None):
        if cursor:
            cursor.close()
        self.connection.close()

    def commit(self):
        self.connection.commit()


