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
        # self.cursor.execute("use cs223; ")
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
        print(f"in worker, commit_counter={self.connection.commit_counter}, query_counter={self.connection.query_counter}")
        self.job_cache.analysis_dict['transaction_counter'] += self.connection.commit_counter
        self.job_cache.analysis_dict['query_counter'] += self.connection.query_counter
        self.job_cache.analysis_dict['insert_duration'] += self.insert_duration
        self.job_cache.analysis_dict['query_duration'] += self.query_duration
        # print(f"a worker finished processing - committed transaction: {self.connection.commit_counter}")

    def close(self):
        self.connection.close(self.cursor)

    def process_transactions(self):
        # if self.get_queue_size() == 0:
        #     return
        start_time = datetime.now()
        if len(self.transaction.sql_list) > 0:
            for sql in self.transaction.sql_list:
                # print(f"execute sql: {sql}")
                self.cursor.execute(sql)
            if "INSERT" in self.transaction.sql_list[0]:
                self.connection.commit()
                end_time = datetime.now()
                self.insert_duration += ((end_time-start_time).total_seconds())
            else:
                self.connection.query_counter += 1
                end_time = datetime.now()
                # print(((end_time - start_time).total_seconds()))
                self.query_duration += ((end_time - start_time).total_seconds())


# if __name__ == '__main__':
#     # test
#     job_cache = JobCache()
#     transaction = Transaction()
#     transaction.sql_list.append("INSERT INTO thermometerobservation VALUES ('95d43c63-74ff-40f2-96cc-c59c40c8d0be', 67, '2017-11-17 13:18:00', 'fe7650c6_f0d2_4e69_81b6_15e96f381814');")
#     transaction.sql_list.append("INSERT INTO thermometerobservation VALUES ('9ed41c63-74ff-40f2-96cc-c59c40c8d0be', 67, '2017-11-07 13:18:00', 'fe7650c6_f0d2_4e69_81b6_15e96f381814');")
#     job_cache.t_queue.put(transaction)
#     transaction = Transaction()
#     transaction.sql_list.append(
#         "INSERT INTO thermometerobservation VALUES ('9ed43c67-74ff-40f2-98cc-c59c40c8d0be', 67, '2017-01-17 13:18:00', 'fe7650c6_f0d2_4e69_81b6_15e96f381814');")
#     transaction.sql_list.append(
#         "INSERT INTO thermometerobservation VALUES ('3ed43c63-74ff-40f2-26cc-c59c40c8d0be', 67, '2017-12-07 13:18:00', 'fe7650c6_f0d2_4e69_81b6_15e96f381814');")
#     job_cache.t_queue.put(transaction)
#     p1 = Worker(db_type='mysql', mpl=4, job_cache=job_cache)
#     p1.start()
#     p1.join()
