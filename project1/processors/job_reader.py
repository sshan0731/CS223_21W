from multiprocessing import Process
from project1.models.transaction import Transaction
from datetime import datetime
from project1.operators.sql_reader import SQLReader
from project1.processors.job_cache import JobCache

epoch_length = 24 * 60 * 60


class JobReader(Process):
    def __init__(self, freq, job_cache: JobCache, operation_type="both", db_type="posegresql"):
        super().__init__()
        self.freq = freq
        self.job_cache = job_cache
        self.operation_type = operation_type
        self.db_type = db_type
        self.observation_file = f"../original_data/data/{self.freq}_concurrency/observation_sorted.sql"
        self.semantic_file = f"../original_data/data/{self.freq}_concurrency/semantic_sorted.sql"
        self.query_file = f"../original_data/queries/{self.freq}_concurrency/queries_sorted.txt"
        if self.db_type == "mysql":
            self.query_file = f"../original_data/queries/{self.freq}_concurrency/queries_mysql_sorted.txt"
        self.current_transaction = Transaction()
        self.epoch_start_time = ""
        self.transaction_dict = {}

    def is_in_epoch_interval(self, timestamp):
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%s')
        if self.epoch_start_time == "":
            self.epoch_start_time = epoch_length * (int(dt) / epoch_length)
            return True
        if (int(dt) - self.epoch_start_time) / epoch_length < 1:
            return True
        self.epoch_start_time = epoch_length * (int(dt) / epoch_length)
        return False

    @staticmethod
    def extract_sensor_id(sql):
        sensor_id = ""
        if "INSERT" in sql:
            temp = sql.split("'")
            sensor_id = temp[-2]
        return sensor_id

    def run(self) -> None:
        if self.operation_type == "insert" or self.operation_type == "both":
            self.read_a_insert_file(self.observation_file)
            self.epoch_start_time = ""
            self.read_a_insert_file(self.semantic_file)
            self.epoch_start_time = ""
        if self.operation_type == "query" or self.operation_type == "both":
            self.read_a_query_file(self.query_file)

    def read_a_query_file(self, file_path):
        sql_reader = SQLReader(file_path)
        while True:
            current_sql = sql_reader.get_next_query_sql()
            if not current_sql:
                break
            self.current_transaction = Transaction()
            self.current_transaction.add_a_sql_to_list(current_sql)
            self.job_cache.add_job(self.current_transaction)
        sql_reader.close()

    def read_a_insert_file(self, file_path):
        sql_reader = SQLReader(file_path)
        while True:
            current_sql = sql_reader.get_next_insert_sql()
            if not current_sql:
                for t in self.transaction_dict.values():
                    self.job_cache.add_job(t)
                self.transaction_dict.clear()
                break

            if self.is_in_epoch_interval(SQLReader.extract_timestamp(current_sql)):
                sensor_id = self.extract_sensor_id(current_sql)
                if sensor_id not in self.transaction_dict:
                    transaction = Transaction()
                    transaction.add_a_sql_to_list(current_sql)
                    self.transaction_dict[sensor_id] = transaction
                else:
                    transaction = self.transaction_dict[sensor_id]
                    transaction.add_a_sql_to_list(current_sql)
                    self.transaction_dict[sensor_id] = transaction
            else:
                for t in self.transaction_dict.values():
                    self.job_cache.add_job(t)
                self.transaction_dict.clear()
                sensor_id = self.extract_sensor_id(current_sql)
                transaction = Transaction()
                transaction.add_a_sql_to_list(current_sql)
                self.transaction_dict[sensor_id] = transaction
        sql_reader.close()