from multiprocessing import Manager
from project1.models.transaction import Transaction


class JobCache:
    def __init__(self):
        self.t_queue = Manager().Queue()
        self.analysis_dict = Manager().dict()
        self.analysis_dict['transaction_counter'] = 0
        self.analysis_dict['query_counter'] = 0
        self.analysis_dict['insert_duration'] = 0
        self.analysis_dict['query_duration'] = 0

    def get_job(self):
        return self.t_queue.get(timeout=10)

    def add_job(self, transaction: Transaction):
        self.t_queue.put(transaction)

    def job_cache_queue_len(self):
        return self.t_queue.qsize()
    # def run(self) -> None:
    #     i = 0
    #     is_end = False
    #     while True:
    #         i += 1
    #         print(f"--------simulator----{i}--{self.job_reader.get_queue_len()}---------")
    #
    #         if self.job_reader.get_queue_len() > 0:
    #             idx = random.randint(len(self.workers) - 1)
    #             print(f"worker-{idx} receives jobs")
    #             self.workers[idx].receive_jobs(self.job_reader.t_queue)
    #             self.job_reader.reset_queue()
    #         else:
    #             print("~~~44546465~~~~~~~~~~~~~~")
    #             counter = 0
    #             while True:
    #                 print("========77777=========================")
    #                 print(f"main --- sleep --- {counter} ---- ")
    #                 time.sleep(100)
    #                 counter += 1
    #                 if counter > 10:
    #                     is_end = True
    #                     break
    #         if is_end:
    #             break