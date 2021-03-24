from project1.processors.job_cache import JobCache
from project1.processors.job_reader import JobReader
from project1.processors.worker import Worker
from datetime import datetime


class Simulator:
    def __init__(self, freq, db_type, mpl, transaction_style='both'):
        self.freq = freq
        self.db_type = db_type
        # self.transaction_style = transaction_style
        self.job_cache = JobCache()
        self.job_reader = JobReader(self.freq, job_cache=self.job_cache, operation_type=transaction_style, db_type=self.db_type)
        self.workers = self.set_workers(mpl)

    def start(self):
        # start_time = datetime.now()
        # print(f"simulation start - {start_time}")
        self.job_reader.start()
        for worker in self.workers:
            # print("worker start...")
            worker.start()

        for worker in self.workers:
            worker.join()
        self.job_reader.join()
        print(f"transaction counter= {self.job_cache.analysis_dict['transaction_counter']}, query counter= {self.job_cache.analysis_dict['query_counter']}, total num = {self.job_cache.analysis_dict['transaction_counter'] + self.job_cache.analysis_dict['query_counter']}")
        print(f"insert duration = {self.job_cache.analysis_dict['insert_duration']}, query duration = {self.job_cache.analysis_dict['query_duration']}")
        print(f"total duration = {self.job_cache.analysis_dict['insert_duration'] + self.job_cache.analysis_dict['query_duration']}")
        if self.job_cache.analysis_dict['insert_duration'] > 0:
            print(f"throughput = {self.job_cache.analysis_dict['transaction_counter'] / self.job_cache.analysis_dict['insert_duration']}")
        if self.job_cache.analysis_dict['query_counter'] > 0:
            print(f"avg response time = {self.job_cache.analysis_dict['query_duration'] / self.job_cache.analysis_dict['query_counter']}")
        print(
            f"total avg response time = {(self.job_cache.analysis_dict['query_duration'] + self.job_cache.analysis_dict['insert_duration']) / (self.job_cache.analysis_dict['query_counter']+self.job_cache.analysis_dict['transaction_counter'])}")

        for worker in self.workers:
            worker.close()

    def set_workers(self, num_of_workers):
        workers = []
        for i in range(1, 1 + num_of_workers):
            worker = Worker(db_type=self.db_type, job_cache=self.job_cache)
            workers.append(worker)
        return workers


if __name__ == '__main__':
    sim = Simulator(freq="low", db_type='postgresql', mpl=1)
    sim.start()
    #
    # for worker in self.workers:
    #     worker.start()
    # self.job_reader.start()
    # is_end = False
    # i = 0
    # while True:
    #     i += 1
    #     print(f"--------simulator----{i}--{self.job_reader.get_queue_len()}---------")
    #     if self.job_reader.get_queue_len() > 0:
    #         idx = random.randint(self.num_of_workers - 1)
    #         print(f"worker-{idx} receives jobs")
    #         self.workers[idx].receive_jobs(self.job_reader.t_queue)
    #         self.job_reader.reset_queue()
    #     else:
    #         print("~~~~~~~~~~~~~~~~~")
    #         counter = 0
    #         while True:
    #             print("=================================")
    #             print(f"main --- sleep --- {counter} ---- ")
    #             time.sleep(100)
    #             counter += 1
    #             if counter > 10:
    #                 is_end = True
    #                 break
    #     if is_end:
    #         break
    # self.job_reader.join()
    # for worker in self.workers:
    #     worker.join()