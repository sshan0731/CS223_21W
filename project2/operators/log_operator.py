from project2.operators.dbconnector import DBConnection


class LogOperator:
    def __init__(self):
        self.connection = DBConnection()
        self.cursor = self.connection.get_cursor()
        with open("original_data/schema/create_log_table.sql", "r") as file:
            self.cursor.execute(file.read())
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def insert_coordinator_log(self, tid, status):
        self.insert_log("COORDINATOR", tid, status)

    def insert_agent_log(self, agent_id, tid, status):
        self.insert_log("AGENT_" + str(agent_id), tid, status)

    def insert_log(self, machine_id, tid, status):
        self.cursor.execute(f"insert into LOG_TABLE(machine_id, tid, status) values('{machine_id}', '{tid}', '{status}');")
        self.connection.commit()

    def query_log(self, machine_name, tid):
        self.cursor.execute(f"select * from log_table where lid = (select max(lid) from log_table where tid = '{tid}' and machine_id='{machine_name}')")
        return self.cursor.fetchall()

    def query_agent_log(self, machine_id, tid):
        agent_name = "AGENT_" + machine_id
        return self.query_log(agent_name, tid)

    def query_coordinator_log(self, tid):
        return self.query_log("COORDINATOR", tid)

    def insert_job_log(self):
        self.insert_log('JOB_READER', 'JOB_READER', 'GET_A_JOB')

    def query_job_log(self):
        return self.query_log('JOB_READER', 'JOB_READER')

    def job_reader_query_max_log_id(self):
        job_log_rows = self.query_log('JOB_READER', 'JOB_READER')
        max_log_id = -1
        if len(job_log_rows) > 0:
            for job_log_row in job_log_rows:
                max_log_id = int(job_log_row[0])
                break
        return max_log_id