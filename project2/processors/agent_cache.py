from multiprocessing import Manager
import time
from project2.operators.log_operator import LogOperator


class AgentCache:
    def __init__(self, num_of_agents):
        self.machine_status_dict = Manager().dict()
        self.num_of_agents = num_of_agents
        self.is_fail_status_dict = Manager().dict()
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