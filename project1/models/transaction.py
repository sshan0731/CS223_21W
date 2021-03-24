
class Transaction:
    def __init__(self):
        self.sql_list = list()
        self.transaction_interval = 1000  # 1s = 1 transaction

    def add_a_sql_to_list(self, sql):
        self.sql_list.append(sql)