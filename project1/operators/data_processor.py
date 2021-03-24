from pathlib import Path
from project1.operators.sql_reader import SQLReader


def sort_file(src_file_path, obj_file_path):
    sql_reader = SQLReader(src_file_path)
    sqls = []
    # i = 0
    while 1:
        # i += 1
        # if i%100 == 0:
        #     print(i)
        sql = sql_reader.get_next_insert_sql()
        if not sql:
            break
        sqls.append(sql)
    sqls.sort(key=sql_reader.extract_timestamp)
    with open(obj_file_path, 'w') as file:
        for sql in sqls:
            file.write(sql + '\n')
    sql_reader.close()


def sort_query_file(src_file_path, obj_file_path):
    print(f"in sort query file {src_file_path}")
    sql_reader = SQLReader(src_file_path)
    queries_with_timestamp = []
    while True:
        query = sql_reader.get_next_query_sql_line()
        # print(query)
        if not query:
            break
        queries_with_timestamp.append(query)
    queries_with_timestamp.sort(key=sql_reader.extract_query_timestamp)
    with open(obj_file_path, 'w') as file:
        for query in queries_with_timestamp:
            file.write(query + '\n')
    sql_reader.close()


def create_mysql_query_file(src_file, obj_file_path):
    # print(src_file)
    # print(obj_file_path)
    sql_reader = SQLReader(src_file)
    queries = []
    while True:
        query = sql_reader.get_next_query_sql_line()
        if not query:
            break
        query = translate_a_sql_to_mysql_format(query)
        queries.append(query)
    with open(obj_file_path, 'w') as file:
        for query in queries:
            file.write(query + '\n')
    sql_reader.close()


def translate_a_sql_to_mysql_format(query):
    if "date_trunc('day', s1.timeStamp)" in query:
        return query.replace("date_trunc('day', s1.timeStamp)", "DATE_FORMAT(s1.timeStamp, 'DD')")
    if "date_trunc('day', s2.timeStamp)" in query:
        return query.replace("date_trunc('day', s2.timeStamp)", "DATE_FORMAT(s2.timeStamp, 'DD')")
    if "date_trunc('day', timestamp)" in query:
        return query.replace("date_trunc('day', timestamp)", "DATE_FORMAT(timeStamp, 'DD')")
    if "date_trunc('day', so.timestamp)" in query:
        return query.replace("date_trunc('day', so.timestamp)", "DATE_FORMAT(so.timeStamp, 'DD')")
    return query

class DataProcessor:
    def __init__(self, freq='low'):
        self.freq = freq
        self.observation_file = f"../original_data/data/{self.freq}_concurrency/observation_sorted.sql"
        self.semantic_file = f"../original_data/data/{self.freq}_concurrency/semantic_sorted.sql"
        self.query_file = f"../original_data/queries/{self.freq}_concurrency/queries_sorted.txt"
        self.mysql_query_file = f"../original_data/queries/{self.freq}_concurrency/queries_mysql_sorted.txt"

    # @staticmethod
    def is_sorted(self, file):
        return Path(file).is_file()
    # def create_sorted_files(self):

    def process_data(self):
        if not self.is_sorted(self.observation_file):
            sort_file(f"../original_data/data/{self.freq}_concurrency/observation_{self.freq}_concurrency.sql",
                      self.observation_file)
        if not self.is_sorted(self.semantic_file):
            sort_file(f"../original_data/data/{self.freq}_concurrency/semantic_observation_{self.freq}_concurrency.sql",
                      self.semantic_file)
        if not self.is_sorted(self.query_file):
            sort_query_file(f"../original_data/queries/{self.freq}_concurrency/queries.txt", self.query_file)
        if not self.is_sorted(self.mysql_query_file):
            create_mysql_query_file(self.query_file, self.mysql_query_file)






if __name__ == '__main__':
    # data_processor = DataProcessor(freq='low')
    # data_processor.process_data()
    create_mysql_query_file(f"../original_data/queries/low_concurrency/queries_sorted.txt",
                            f"../original_data/queries/low_concurrency/queries_mysql_sorted.txt")

