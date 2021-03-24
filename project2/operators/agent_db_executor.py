import psycopg2
from project2.operators.dbconnector import DBConnection
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED


class AgentDBExecutor:
    def __init__(self, agent_id):
        self.connection = DBConnection(id=agent_id)
        self.cursor = self.connection.get_cursor()
        self.connection.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        self.tID = ""
        self.agent_id = agent_id

    def reconnect_db(self):
        self.close()
        self.connection = DBConnection(id=self.agent_id)

    def close(self):
        self.cursor.close()
        self.connection.close()

    def simple_commit(self, sql):
        self.cursor.execute(sql)
        self.connection.commit()

    def execute_sql(self, sql):
        self.cursor.execute(sql)

    def set_tid(self, tid):
        self.tID = tid

    def get_tid(self):
        return self.tID

    def prepare_transaction(self):
        sql = "PREPARE TRANSACTION '" + str(self.agent_id) + "_" + self.tID + "';"
        try:
            self.cursor.execute(sql)
            print(f"in prepare_transaction {str(self.agent_id)}_{self.tID} success")
            return True
        except psycopg2.OperationalError as err:
            print(f"in prepare_transaction {str(self.agent_id)}_{self.tID} fails {err}")
            return False

    def rollback_transaction(self):
        sql = "ROLLBACK PREPARED '" + str(self.agent_id) + "_" + self.tID + "';"
        try:
            self.cursor.execute(sql)
            print(f"in rollback_transaction {str(self.agent_id)}_{self.tID} success")
            self.connection.commit()
            return True
        except psycopg2.OperationalError as err:
            print(f"in rollback_transaction {str(self.agent_id)}_{self.tID} fails {err}")
            return False

    def commit_transaction(self):
        sql = "COMMIT PREPARED '" + str(self.agent_id) + "_" + self.tID + "';"
        try:
            self.cursor.execute(sql)
            print(f"in commit_transaction {str(self.agent_id)}_{self.tID} success")
            self.connection.commit()
            return True
        except psycopg2.OperationalError as err:
            print(f"in commit_transaction {str(self.agent_id)}_{self.tID} fails {err}")
            return False
