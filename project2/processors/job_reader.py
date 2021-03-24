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
        # print(f"queue len = {self.job_cache_queue_len()}")

    def is_in_epoch_interval(self, sql):
        timestamp = SQLReader.extract_timestamp(sql)
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%s')
        # print(f"in is_in_epoch_interval: {int(dt)}")
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
        i = 0
        while True:
            i += 1
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
        print(f"sql_reader.close()")
        sql_reader.close()


# if __name__ == '__main__':
    # test function is_in_epoch_interval()
    # sql = "INSERT INTO thermometerobservation VALUES ('9ed43c63-74ff-40f2-96cc-c59c40c8d0be', 67, '2017-11-17 13:18:00', 'fe7650c6_f0d2_4e69_81b6_15e96f381814');"
    # reader = JobReader('mysql', 'low')
    # print(f"{reader.is_in_epoch_interval(sql)} --- {reader.epoch_start_time}")  # should be true
    # sql = "INSERT INTO thermometerobservation VALUES ('9ed43c63-74ff-40f2-96cc-c59c40c8d0be', 67, '2017-11-17 13:18:10', 'fe7650c6_f0d2_4e69_81b6_15e96f381814');"
    # print(f"{reader.is_in_epoch_interval(sql)} --- {reader.epoch_start_time}")  # should be true
    # sql = "INSERT INTO thermometerobservation VALUES ('9ed43c63-74ff-40f2-96cc-c59c40c8d0be', 67, '2018-11-17 13:18:10', 'fe7650c6_f0d2_4e69_81b6_15e96f381814');"
    # print(f"{reader.is_in_epoch_interval(sql)} --- {reader.epoch_start_time}")  # should be false

    # test read jobs
    reader = JobReader(db_type='mysql', freq='low')
    reader.start()