import time
from multiprocessing import Manager
from project2.models.transaction import Transaction
from project2.operators.sql_reader import SQLReader
from project2.processors.agent_cache import AgentCache
from project2.processors.job_reader import JobReader
from project2.processors.agent import Agent
from queue import Empty
import datetime


# worker id starts from 1
class Simulator:
    def __init__(self, freq='low', num_of_agents=3):
        self.agent_db_executor_dict = {}
        self.freq = freq
        self.job_reader = JobReader(self.freq)
        self.agent_cache = AgentCache(num_of_agents)
        self.agent_sql_queue_dict = {}
        self.agents = self.set_agents(num_of_agents)
        self.transaction = Transaction()
        self.to_recover = False
        self.is_finished = False

    def start(self):

        self.job_reader.start()

        while True:
            if self.job_reader.get_queue_len() > 0:
                self.transaction = self.job_reader.get_job()
                self.agent_cache.log_operator.insert_job_log()
                break
        for agent in self.agents:
            agent.start()
        self.agent_cache.is_fail_status_dict[0] = False  # coordinator status
        while not self.is_finished:
            while self.is_fail():
                if not self.to_recover:
                    self.to_recover = True
                self.agent_cache.sleep_for_ms(1)
            if self.to_recover:
                self.coordinator_recover()

            self.agent_cache.set_tid(self.generate_a_specific_tid())
            self.reset_machine_status()
            self.allocate_current_transaction()  # put sqls into agent cache
            self.coordinator_send_message("PREPARE")
            self.wait_agents_to_process_sqls()
            print("in coordinator, after agents finish processing sqls")
            vote_res = self.get_vote_result()
            self.coordinator_send_message(vote_res)
            print("in coordinator, after set_coordinator_status")
            self.wait_agents_to_return_ack()
            print("in coordinator, after wait_agents_to_return_ack")
            self.coordinator_send_message("ACKNOWLEDGED")
            try:
                self.transaction = self.job_reader.get_job()
                self.agent_cache.log_operator.insert_job_log()
            except Empty:
                self.is_finished = True

        for i in range(1, self.agent_cache.num_of_agents + 1):
            self.agent_cache.machine_status_dict[i] = "FINISH"

        for worker in self.agents:
            worker.join()
        self.job_reader.join()
        self.close()

    def coordinator_recover(self):
        self.agent_cache.is_fail_status_dict[0] = False
        self.to_recover = False
        crash_status = self.coordinator_get_crash_status()
        self.coordinator_recover_process(crash_status)

    def coordinator_recover_process(self, crash_status):
        if crash_status == 0:
            self.agent_cache.set_tid(self.generate_a_specific_tid())
            self.reset_machine_status()
            self.allocate_current_transaction()  # put sqls into agent cache
            self.coordinator_send_message("PREPARE")
        if crash_status <= 1:
            self.wait_agents_to_process_sqls()
            print("in coordinator, after agents finish processing sqls")
            vote_res = self.get_vote_result()
            self.coordinator_send_message(vote_res)
            print("in coordinator, after set_coordinator_status")
        if crash_status <= 2:
            self.wait_agents_to_return_ack()
            print("in coordinator, after wait_agents_to_return_ack")
            self.coordinator_send_message("ACKNOWLEDGED")
        if crash_status <= 3:
            try:
                self.transaction = self.job_reader.get_job()
                self.agent_cache.log_operator.insert_job_log()

            except Empty:
                self.is_finished = True

    def coordinator_get_crash_status(self):
        """
        to avoid cache manager agent_cache to crash down, here we read log from db
        'id=' + str(row[0]) + ' machine =' + str(row[1]) + ' tid = ' + str(row[2]) + ' status =' + str(row[3])
        """
        crash_status = -1
        coordinator_log_rows = self.agent_cache.log_operator.query_coordinator_log(self.agent_cache.get_tid())
        if str(coordinator_log_rows[0][3]) == "INITIATE":
            crash_status = 0  # no need to recover
        if str(coordinator_log_rows[0][3]) == "PREPARE":
            crash_status = 1
        if str(coordinator_log_rows[0][3]) == "COMMIT" or str(coordinator_log_rows[3]) == "ABORT":
            crash_status = 2
        if str(coordinator_log_rows[0][3]) == "ACKNOWLEDGED":
            crash_status = 3
            coordinator_max_log_id = int(coordinator_log_rows[0][0])
            job_log_rows = self.agent_cache.log_operator.job_reader_query_max_log_id()
            job_reader_max_log_id = int(job_log_rows[0][0])
            if job_reader_max_log_id > coordinator_max_log_id:
                crash_status = 4
        return crash_status

    def is_fail(self):
        return self.agent_cache.is_fail_status_dict[0]

    def coordinator_send_message(self, msg):
        self.agent_cache.set_coordinator_status(msg)
        self.agent_cache.log_operator.insert_coordinator_log(self.agent_cache.get_tid(), msg)

    def close(self):
        for worker in self.agents:
            worker.close()
        self.agent_cache.close()

    def reset_machine_status(self):
        self.coordinator_send_message("INITIATE")
        for i in range(1, self.agent_cache.num_of_agents + 1):
            self.agent_cache.set_machine_status_dict(i, "INITIATE")
            self.agent_cache.log_operator.insert_agent_log(i, self.agent_cache.get_tid(), "INITIATE")

    def wait_agents_to_return_ack(self):
        time.sleep(1)
        while True:
            if self.count_num_of_a_status("ACKNOWLEDGE") == self.agent_cache.num_of_agents:
                break

    def count_num_of_a_status(self, status):
        counter = 0
        for i in range(1, self.agent_cache.num_of_agents + 1):
            if self.check_machine_status(i, status):
                counter += 1
        return counter

    def check_machine_status(self, index, status):
        return self.agent_cache.machine_status_dict[index] == status

    def get_vote_result(self):
        if self.count_num_of_a_status("COMMIT") == self.agent_cache.num_of_agents:
            return "COMMIT"
        return "ABORT"

    def generate_a_specific_tid(self):
        return str(datetime.datetime.now().strftime('%s')) + str(hash(self.transaction.sql_list[0]))

    def allocate_current_transaction(self):
        for sql in self.transaction.sql_list:
            hash_id = SQLReader.get_hash_val(sql, self.agent_cache.num_of_agents)
            self.agent_sql_queue_dict[hash_id].put(sql)

    def wait_agents_to_process_sqls(self):
        while True:
            is_transanction_finished = True
            for i in range(1, self.agent_cache.num_of_agents + 1):
                if self.agent_cache.get_machine_status(i) != "ABORT" and self.agent_cache.get_machine_status(i) != "COMMIT":
                    is_transanction_finished = False
                    break
            if is_transanction_finished:
                break

    def set_agents(self, num_of_agents):
        agents = []
        for i in range(1, 1 + num_of_agents):
            queue = Manager().Queue()
            agent = Agent(agent_id=i, agent_cache=self.agent_cache, queue=queue)
            self.agent_sql_queue_dict[agent.agent_id] = queue
            agents.append(agent)
        return agents