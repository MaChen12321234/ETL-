import logging
from config import etl_config as conf


class Logging(object):
    # 定义日志等级
    def __init__(self, level=20):
        self.logger = logging.getLogger()
        self.logger.setLevel(level)


# 构建一个方法，通过这个方法给日志对象设置格式等
def init_log():
    # 实例化对象
    logger = Logging().logger

    if logger.handlers:
        return logger

    # 设置日志输出至文件夹
    file_handle = logging.FileHandler(
        filename=conf.log_file,
        mode='a',
        encoding='utf-8'
    )

    # 设置format输出格式
    fmt = logging.Formatter(
        "%(asctime)s - [%(levelname)s] - %(filename)s[%(lineno)d]: %(message)s"
    )

    # 将格式写入handle中
    file_handle.setFormatter(fmt)

    # 再讲handle写给日志对象logger
    logger.addHandler(file_handle)

    return logger



