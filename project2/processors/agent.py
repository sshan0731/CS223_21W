from multiprocessing import Process
import numpy as np
from project2.operators.agent_db_executor import AgentDBExecutor
from project2.processors.agent_cache import AgentCache


class Agent(Process):
    def __init__(self, agent_id, agent_cache: AgentCache, queue):
        super().__init__()
        self.t_list = list()
        self.agent_id = agent_id
        self.agent_cache = agent_cache
        self.agent_name = "Agent_" + str(np.random.randint(1, high=100000))  # genratate random name
        self.agent_db_executor = AgentDBExecutor(self.agent_id)
        self.sql = ""
        self.sql_queue = queue  # Manager().Queue()
        self.agent_cache.machine_status_dict[agent_id] = "INITIATE"
        self.agent_cache.is_fail_status_dict[agent_id] = False
        self.to_recover = False

    def close(self):
        self.agent_db_executor.close()

    def run(self) -> None:
        while 1:
            while self.is_fail():
                if not self.to_recover:
                    self.to_recover = True
                self.agent_cache.sleep_for_ms(1)

            if self.to_recover:
                self.agent_recover()

            if self.agent_cache.get_machine_status(self.agent_id) == "FINISH":
                print(f"in agent {self.agent_id} -- status = FINISH")
                break
            elif self.agent_cache.get_coordinator_status() == "PREPARE" and self.get_agent_status() == "INITIATE":
                self.agent_prepare_and_vote()
            elif self.agent_cache.get_coordinator_status() == "COMMIT" and self.get_agent_status() == "COMMIT":  # and not self.agent_db_executor.is_job_finished():
                self.agent_commit_and_ack()
            elif self.agent_cache.get_coordinator_status() == "ABORT" and (self.get_agent_status() == "ABORT" or self.get_agent_status() == "COMMIT") :  # not self.agent_db_executor.is_job_finished():
                self.agent_rollback_and_ack()

    def agent_recover(self):
        self.agent_cache.is_fail_status_dict[self.agent_id] = False
        self.to_recover = False
        self.get_crash_status_and_recover()

    def get_crash_status_and_recover(self):
        """
        to avoid cache manager agent_cache to crash down, here we read log from db
        'id=' + str(row[0]) + ' machine =' + str(row[1]) + ' tid = ' + str(row[2]) + ' status =' + str(row[3])
        """
        coordinator_log_rows = self.agent_cache.log_operator.query_coordinator_log(self.agent_cache.get_tid())
        agent_rows = self.agent_cache.log_operator.query_agent_log(self.agent_id, self.agent_cache.get_tid())
        if str(coordinator_log_rows[0][3]) == "INITIATE" or str(coordinator_log_rows[0][3]) == "ACKNOWLEDGED":
            return  # no need to recover
        if str(coordinator_log_rows[0][3]) == "PREPARE":
            if str(agent_rows[0][3]) == "INITIATE":
                self.agent_prepare_and_vote()
                return
            if str(agent_rows[0][3]) == "COMMIT" or str(agent_rows[0][3]) == "ABORT":
                return  # have finished prepare transactions; need to do nothing
        if str(coordinator_log_rows[0][3]) == "COMMIT" or str(coordinator_log_rows[0][3]) == "ABORT":
            if str(agent_rows[0][3]) == "COMMIT_A_TRANSACTION" or str(agent_rows[0][3]) == "ABORT_A_TRANSACTION":
                self.agent_send_message("ACKNOWLEDGE")
                return
            if str(agent_rows[0][3]) == "ACKNOWLEDGE":
                return  # need to do nothing
            if str(coordinator_log_rows[0][3]) == "COMMIT":
                self.agent_commit_and_ack()
                return
            self.agent_rollback_and_ack()  # abort
            return

    def agent_send_message(self, msg):
        self.set_agent_status(msg)
        self.agent_cache.log_operator.insert_agent_log(self.agent_id, self.agent_cache.get_tid(), msg)

    def agent_rollback_and_ack(self):
        if not self.is_fail():
            self.agent_db_executor.rollback_transaction()
            self.agent_cache.log_operator.insert_agent_log(self.agent_id, self.agent_cache.get_tid(), "ABORT_A_TRANSACTION")
        if not self.is_fail():
            self.agent_send_message("ACKNOWLEDGE")

    def agent_commit_and_ack(self):
        if not self.is_fail():
            self.agent_db_executor.commit_transaction()
        if not self.is_fail():
            self.agent_send_message("ACKNOWLEDGE")

    def agent_prepare_and_vote(self):
        self.agent_db_executor.set_tid(self.agent_cache.get_tid())
        while self.sql_queue.qsize() > 0 and not self.is_fail():
            sql = self.get_a_sql()
            self.agent_db_executor.execute_sql(sql)
        if not self.is_fail():
            if self.agent_db_executor.prepare_transaction():
                if not self.is_fail():
                    self.agent_send_message("COMMIT")
            else:
                print(f"agent{self.agent_id} set status abort")
                if not self.is_fail():
                    self.agent_send_message("ABORT")

    def is_fail(self):
        return self.agent_cache.is_fail_status_dict[self.agent_id]

    def set_agent_status(self, status):
        self.agent_cache.set_machine_status_dict(self.agent_id, status)

    def get_agent_status(self):
        return self.agent_cache.get_machine_status(self.agent_id)

    def get_a_sql(self):
        return self.sql_queue.get(timeout=10)
