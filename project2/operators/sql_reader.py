import re


class SQLReader:
    def __init__(self, path):
        self.file = open(path, "r")

    def get_next_insert_sql(self):
        line = ""
        while "INSERT" not in line:
            line = self.file.readline()
            if not line:
                break
        return line

    def close(self):
        self.file.close()

    @staticmethod
    def extract_timestamp(sql: str):
        match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', sql)
        return match.group()

    @staticmethod
    def extract_sensor_id(sql: str):
        x = sql.split("'")
        return x[len(x)-2]

    @staticmethod
    def get_hash_val(sql: str, num_of_agents):
        return hash(SQLReader.extract_sensor_id(sql) + SQLReader.extract_timestamp(sql)) % num_of_agents + 1 # worker id starts from 1


if __name__ == '__main__':
    SQLReader.get_hash_val("INSERT INTO wemoobservation VALUES ('8840b16c-6c12-4e27-90a1-96e561217369', 7, 1112, '2017-11-15 03:57:00', '974c0fb1_94c6_4cfa_a004_9a512f634683');", 11)

