from pathlib import Path
from project1.operators.sql_reader import SQLReader


def sort_file(src_file_path, obj_file_path):
    sql_reader = SQLReader(src_file_path)
    sqls = []
    while 1:
        sql = sql_reader.get_next_insert_sql()
        if not sql:
            break
        sqls.append(sql)
    sqls.sort(key=sql_reader.extract_timestamp)
    with open(obj_file_path, 'w') as file:
        for sql in sqls:
            file.write(sql + '\n')
    sql_reader.close()


class DataProcessor:
    def __init__(self, freq='low'):
        self.freq = freq
        self.observation_file = f"original_data/data/{self.freq}_concurrency/observation_sorted.sql"
        self.semantic_file = f"original_data/data/{self.freq}_concurrency/semantic_sorted.sql"

    def is_sorted(self):
        print(self.observation_file)
        print(self.semantic_file)
        return Path(self.observation_file).is_file() and Path(self.semantic_file).is_file()

    def create_sorted_files(self):
        print("to sort file...")
        sort_file(f"original_data/data/{self.freq}_concurrency/observation_{self.freq}_concurrency.sql",
                  self.observation_file)
        sort_file(f"original_data/data/{self.freq}_concurrency/semantic_observation_{self.freq}_concurrency.sql",
                  self.semantic_file)

    def process_data(self):
        if not self.is_sorted():
            self.create_sorted_files()
