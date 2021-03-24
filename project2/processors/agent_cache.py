from multiprocessing import Manager
from queue import Queue
import time
from project2.models.transaction import Transaction
from project2.operators.log_operator import LogOperator


class AgentCache:
    def __init__(self, num_of_agents):
        # self.t_queue = Manager().Queue()
        self.machine_status_dict = Manager().dict()
        self.num_of_agents = num_of_agents
        self.is_fail_status_dict = Manager().dict()
        # self.agent_sql_queues = Manager().dict()
        # self.sql_queue_dict = {}  # Manager().dict()
        # self.num_of_agents = num_of_agents
        self.log_operator = LogOperator()

    @staticmethod
    def sleep_for_ms(ms):
        while True:
            if int(time.time()) % ms == 0:
                break

    def close(self):
        self.log_operator.close()

    def reset_machine_status_dict(self, num_of_agents):
        pass

    def set_machine_status_dict(self, index, status):
        self.machine_status_dict[index] = status

    def get_machine_status(self, index):
        return self.machine_status_dict[index]

    def set_coordinator_status(self, status):
        print(f"set coordinator status {status}")
        self.set_machine_status_dict(0, status)

    def get_coordinator_status(self):
        return self.get_machine_status(0)

    def set_tid(self, tid):
        print(f"tid = {tid}")
        self.machine_status_dict['tid'] = tid

    def get_tid(self):
        return self.machine_status_dict['tid']


    # def get_a_sql(self, agent_id):
    #     print(f"----queue len = {self.agent_i_sql_queue_len(agent_id)}")
    #     return self.agent_sql_queues[agent_id].get(timeout=10)
    #
    # def add_job(self, transaction: Transaction):
    #     self.t_queue.put(transaction)
    #     print(f"queue len = {self.job_cache_queue_len()}")

    # def agent_i_sql_queue_len(self, idx):
    #     # print(f"self.agent_sql_queues[idx].qsize() = {self.sql_queue_dict[idx].qsize()}")
    #     q = Queue(self.sql_queue_dict[idx])
    #     return q.qsize()


if __name__ == '__main__':
    agent_cache = AgentCache()
    agent_cache.set_machine_status_dict(1, "aaa")
    print(agent_cache.machine_status_dict[1])
