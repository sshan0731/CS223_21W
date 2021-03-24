import re


class SQLReader:
    def __init__(self, path):
        self.file = open(path, "r")
        # print(f"--- {self.file}")

    def get_next_insert_sql(self):
        line = ""
        while "INSERT" not in line:
            line = self.file.readline()
            if not line:
                break
            # print(line)
        return line

    def get_next_query_sql_line(self):
        line = ""
        while "SELECT" not in line:
            line = self.file.readline().strip()
            if not line:
                break
        return line

    def process_query_get_next_query_with_timestamp(self):
        line = ""
        while True:
            temp_line = self.file.readline().strip()
            if not temp_line:
                break
            if "Z,\"" in temp_line:
                line = temp_line

            else:
                line = line + " " + temp_line
            if temp_line.strip() == "\"":
                break
        return line

    @staticmethod
    def remove_time_in_a_query_line(query_line):
        if "SELECT" in query_line:
            return query_line.split('"')[-2]
        return query_line

    # @staticmethod
    # def remove_time_in_a_query_line(query_line):
    #     return query_line.split('"')[-2]


    def get_next_query_sql(self):
        # print(f"in reader: {self.get_next_query_sql_line()}")
        return self.remove_time_in_a_query_line(self.get_next_query_sql_line())

    def close(self):
        self.file.close()

    @staticmethod
    def extract_timestamp(sql: str):
        match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', sql)
        return match.group()

    @staticmethod
    def extract_query_timestamp(sql: str):
        match = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', sql)
        # str2 = str(match.group())
        # str2.replace("T", " ")
        return match.group().replace("T", " ")


if __name__ == '__main__':
    print(SQLReader.extract_query_timestamp("2017-11-08T00:00:00Z,\"SELECT ci.INFRASTRUCTURE_ID FROM SENSOR sen, COVERAGE_INFRASTRUCTURE ci WHERE sen.id=ci.SENSOR_ID AND sen.id='78dd9081_14a5_41eb_8632_14e45a6b1e57'"))
    # AA = SQLReader.remove_time_in_a_query_line("2017-11-08T00:00:00Z,\" SELECT ci.INFRASTRUCTURE_ID FROM SENSOR sen, COVERAGE_INFRASTRUCTURE ci WHERE sen.id=ci.SENSOR_ID AND sen.id='78dd9081_14a5_41eb_8632_14e45a6b1e57' \"")
    # print(AA)