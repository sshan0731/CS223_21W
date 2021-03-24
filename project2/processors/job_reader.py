from multiprocessing import Process

from project2.models.transaction import Transaction
from datetime import datetime
from multiprocessing import Manager
from project2.operators.sql_reader import SQLReader

epoch_length = 24 * 60 * 60


class JobReader(Process):
    def __init__(self, freq):
        super().__init__()
        self.freq = freq
        self.t_queue = Manager().Queue()
        self.observation_file = f"original_data/data/{self.freq}_concurrency/observation_sorted.sql"
        self.semantic_file = f"original_data/data/{self.freq}_concurrency/semantic_sorted.sql"
        self.current_transaction = Transaction()
        self.epoch_start_time = ""

    def get_queue_len(self):
        return self.t_queue.qsize()

    def get_job(self):
        return self.t_queue.get(timeout=10)

    def add_job(self, transaction: Transaction):
        self.t_queue.put(transaction)

    def is_in_epoch_interval(self, sql):
        timestamp = SQLReader.extract_timestamp(sql)
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%s')
        if self.epoch_start_time == "":
            self.epoch_start_time = epoch_length * (int(dt) / epoch_length)
            return True
        if (int(dt) - self.epoch_start_time) / epoch_length < 1:
            return True
        self.epoch_start_time = epoch_length * (int(dt) / epoch_length)
        return False

    def run(self) -> None:
        print("in job reader: read jobs...")
        self.read_a_file(self.observation_file)
        self.read_a_file(self.semantic_file)

    def read_a_file(self, file):
        sql_reader = SQLReader(file)
        while True:
            current_sql = sql_reader.get_next_insert_sql()
            if not current_sql:
                self.add_job(self.current_transaction)
                break

            if self.is_in_epoch_interval(current_sql):
                self.current_transaction.add_a_sql_to_list(current_sql)
            else:
                self.add_job(self.current_transaction)
                self.current_transaction = Transaction()
                self.current_transaction.add_a_sql_to_list(current_sql)
        sql_reader.close()