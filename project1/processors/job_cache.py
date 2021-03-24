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