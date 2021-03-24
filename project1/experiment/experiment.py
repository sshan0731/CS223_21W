from pathlib import Path
from project1.operators.data_processor import DataProcessor
from project1.operators.dbconnector import DBConnection
from project1.processors.simulator import Simulator

isolation_level_dict = {1: 'SERIALIZABLE', 2: 'REPEATABLE  READ', 3: 'READ COMMITTED', 4: 'READ UNCOMMITTED'}


# SERIALIZABLE | REPEATABLE  READ | READ COMMITTED | READ UNCOMMITTED


def run_script_mysql(connection, cursor, file_path):
    file = open(file_path)
    sql = file.read()
    for result in cursor.execute(sql, multi=True):
        pass
        # if result.with_rows:
        #     print("Rows produced by statement '{}':".format(
        #         result.statement))
        #     print(result.fetchall())
        # else:
        #     print("Number of rows affected by statement '{}': {}".format(
        #         result.statement, result.rowcount))
    connection.commit()


class Experiment:
    def __init__(self, db_type='postgresql', freq='low', isolation_level=2, mpl=5, transaction_style='both'):
        self.db_type = db_type
        self.freq = freq
        self.mpl = mpl
        self.transaction_style = transaction_style
        if transaction_style != "query":
            self.run_experiment_create_db()
        else:
            connection = DBConnection(db_type=self.db_type, mpl=self.mpl)  # set mpl
            connection.close()
        DataProcessor(freq=self.freq).process_data()
        self.set_isolation_level(isolation_level_dict[isolation_level])

    def run_experiment_create_db(self):
        # mpl for mysql connection: Size can not be changed for active pools.
        connection = DBConnection(db_type=self.db_type, mpl=self.mpl)
        cursor = connection.get_cursor()
        if self.db_type == 'mysql':
            cursor.execute("drop database if exists cs223; ")
            connection.commit()
            cursor.execute("create database cs223; ")
            connection.commit()
            cursor.execute("use cs223; ")
            run_script_mysql(connection, cursor, "../original_data/schema/create.sql")
            metadata_file = f"../original_data/data/{self.freq}_concurrency/metadata_mysql.sql"
            if not Path(metadata_file).is_file():
                fin = open(f"../original_data/data/{self.freq}_concurrency/metadata.sql")
                a = fin.readlines()
                fout = open(metadata_file, 'w')
                b = ''.join(a[18:])
                fout.write(b)
                fin.close()
                fout.close()
            run_script_mysql(connection, cursor, metadata_file)

        else:
            for f in list(("../original_data/schema/drop.sql",
                           "../original_data/schema/create.sql",
                           f"../original_data/data/{self.freq}_concurrency/metadata.sql")):
                with open(f, "r") as file:
                    cursor.execute(file.read())
                    connection.commit()
        cursor.close()
        connection.close()

    def set_isolation_level(self, isolation_level):
        connection = DBConnection(self.db_type)
        cursor = connection.get_cursor()
        cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
        connection.commit()
        cursor.close()
        connection.close()

    def run_experiment(self):
        simulator = Simulator(freq=self.freq, db_type=self.db_type, mpl=self.mpl,
                              transaction_style=self.transaction_style)
        simulator.start()


def run_exp1_varying_mpl():
    freq = 'low'
    iso_idx = 2
    for db_type in ['mysql', 'postgresql']:
        for mpl in [2, 4, 6, 8, 10]:
            exp = Experiment(db_type=db_type, freq=freq, isolation_level=iso_idx, mpl=mpl, transaction_style='both')
            print(f"db_type={db_type}, freq={freq}, iso_level={iso_idx}, mpl={mpl}, transaction='both'")
            exp.run_experiment()
            print("--------------")


def run_exp2_varying_isolation_level():
    freq = 'low'
    mpl = 4
    for db_type in ['postgresql', 'mysql']:
        for iso_idx in range(1, 5):
            exp = Experiment(db_type=db_type, freq=freq, isolation_level=iso_idx, mpl=mpl, transaction_style='both')
            print(f"db_type={db_type}, freq={freq}, iso_level={iso_idx}, mpl={mpl}, transaction='both'")
            exp.run_experiment()
            print("--------------")


def run_exp3_varying_freq():
    mpl = 4
    iso_idx = 2
    for db_type in ['mysql', 'postgresql']:
        for freq in ['low', 'high']:
            exp = Experiment(db_type=db_type, freq=freq, isolation_level=iso_idx, mpl=mpl, transaction_style='both')
            print(f"db_type={db_type}, freq={freq}, iso_level={iso_idx}, mpl={mpl}, transaction='both'")
            exp.run_experiment()
            print("--------------")


if __name__ == '__main__':
    # run_exp1_varying_mpl()
    run_exp2_varying_isolation_level()
    # run_exp3_varying_freq()