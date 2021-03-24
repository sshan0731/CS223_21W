from pathlib import Path
from project2.operators.data_processor import DataProcessor
from project2.operators.dbconnector import DBConnection
from project2.processors.simulator import Simulator


class Experiment:
    def __init__(self, freq='low', num_of_agents=2):
        self.freq = freq
        self.num_of_agents = num_of_agents
        self.run_experiment_create_db()
        print("create db finished")
        DataProcessor(freq=self.freq).process_data()

    def run_experiment_create_db(self):
        connection = DBConnection(id=-1)
        cursor = connection.get_cursor()
        connection.set_isolation_level('auto')  # <-- ADD THIS LINE
        for agent_id in range(1, self.num_of_agents+1):
            cursor.execute(f"select * from pg_database where datname='cs223_{agent_id}';")
            res = cursor.fetchall()
            if len(res) == 0:
                print(f"CREATE DATABASE cs223_{agent_id};")
                cursor.execute(f"CREATE DATABASE cs223_{agent_id};")
        cursor.close()
        connection.close()

        for agent_id in range(1, self.num_of_agents+1):
            connection = DBConnection(id=agent_id)
            cursor = connection.get_cursor()
            for f in list(("original_data/schema/drop.sql",
                           "original_data/schema/create.sql",
                           f"original_data/data/{self.freq}_concurrency/metadata.sql")):
                with open(f, "r") as file:
                    cursor.execute(file.read())
                    connection.commit()
            cursor.close()
            connection.close()

    def run_experiment(self):
        simulator = Simulator(self.freq, num_of_agents=self.num_of_agents)
        simulator.start()


if __name__ == '__main__':
    freq = 'low'
    exp = Experiment(freq=freq, num_of_agents=5)
    exp.run_experiment()
