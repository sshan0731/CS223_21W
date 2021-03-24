import time
from queue import Empty
from project1.models.transaction import Transaction
from multiprocessing import Process
from datetime import datetime
from project1.operators.dbconnector import DBConnection
from project1.processors.job_cache import JobCache


def sleep_for_ms(ms):
    while True:
        if int(time.time()) % ms == 0:
            break


class Worker(Process):
    def __init__(self, db_type, job_cache: JobCache):
        super().__init__()
        self.t_list = list()
        self.db_type = db_type
        self.job_cache = job_cache
        self.connection = DBConnection(db_type=db_type)
        self.cursor = self.connection.get_cursor()
        self.transaction = Transaction()
        self.query_duration = 0
        self.insert_duration = 0

    def run(self) -> None:
        while True:
            if self.job_cache.job_cache_queue_len() > 0:
                self.transaction = self.job_cache.get_job()
                break
        is_finished = False
        while not is_finished:
            self.process_transactions()
            try:
                self.transaction = self.job_cache.get_job()
            except Empty:
                counter = 0
                is_finished = True
                while counter < 10:
                    sleep_for_ms(1)
                    try:
                        self.transaction = self.job_cache.get_job()
                        is_finished = False
                        break
                    except Empty:
                        counter += 1

        self.job_cache.analysis_dict['transaction_counter'] += self.connection.commit_counter
        self.job_cache.analysis_dict['query_counter'] += self.connection.query_counter
        self.job_cache.analysis_dict['insert_duration'] += self.insert_duration
        self.job_cache.analysis_dict['query_duration'] += self.query_duration

    def close(self):
        self.connection.close(self.cursor)

    def process_transactions(self):
        start_time = datetime.now()
        if len(self.transaction.sql_list) > 0:
            for sql in self.transaction.sql_list:
                self.cursor.execute(sql)
            if "INSERT" in self.transaction.sql_list[0]:
                self.connection.commit()
                end_time = datetime.now()
                self.insert_duration += ((end_time-start_time).total_seconds())
            else:
                self.connection.query_counter += 1
                end_time = datetime.now()
                self.query_duration += ((end_time - start_time).total_seconds())