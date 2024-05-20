"""
此模型文件记录logger需求的数据模型
"""
from config import etl_config as conf


class LoggerServiceModel(object):
    # logger_time, logger_level, code_module, response_time, province, city, logger_manager
    def __init__(self, data: list):
        self.logger_time = data[0]
        self.logger_level = data[1]
        self.code_module = data[2]
        self.response_time = data[3]
        self.province = data[4]
        self.city = data[5]
        self.logger_manager = data[6]

    #
    def generate_insert_sql(self):
        query_sql = f"INSERT INTO {conf.target_logger_service_table_name}(" \
                    f"logger_time, logger_level, code_module, response_time, province, city, logger_manager) VALUES " \
                    f"('{self.logger_time}'," \
                    f"'{self.logger_level}'," \
                    f"'{self.code_module}'," \
                    f"'{self.response_time}'," \
                    f"'{self.province}'," \
                    f"'{self.city}'," \
                    f"'{self.logger_manager}')"
        return query_sql

    def to_csv(self, sep=","):
        csv_line = \
            f"{self.logger_time}{sep}" \
            f"{self.logger_level}{sep}" \
            f"{self.code_module}{sep}" \
            f"{self.response_time}{sep}" \
            f"{self.province}{sep}" \
            f"{self.city}{sep}" \
            f"{self.logger_manager}"
        return csv_line


