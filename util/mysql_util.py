# 数据库连接工具
"""
    负责提供基本的操作MySQL的方法，如：
    - 创建连接
    - 执行SQL语句
    - 执行SQL查询
    - 查看表是否存在
    - 创建表
    - 开启事务
    - 关闭事务
    - 回滚事务
    - 关闭连接
    - 执行insert语句
    等
"""
import pymysql
from config import etl_config as conf
from util import logging_util


class MysqlUtil:
    # 创建一个数据库连接类，定义数据库连接属性
    def __init__(self, host=conf.metadata_host, user=conf.metadata_user, password=conf.metadata_password,
                 port=conf.metadata_port, autocommit=False, charset=conf.charset):
        """
        只需要创建一个数据库对象，那么只要这个对象存在，数据库连接就会一直存在
        """
        self.conn = pymysql.Connection(
            host=host,
            user=user,
            password=password,
            port=port,
            autocommit=autocommit,
            charset=charset
        )
        self.logger = logging_util.init_log()
        self.logger.info(f'构建数据库{host}：{port}的连接成功')

    def auto_commit_false(self, sql):
        """
        执行无返回结果的查询，不论autocommit是什么参数，都强制提交
        :param sql: 执行sql的语句
        :return:
        """
        # 创建游标
        cursor = self.conn.cursor()
        cursor.execute(sql)
        if not self.conn.get_autocommit():
            # 没有配置自动提交参数，进入此地
            self.conn.commit()
        cursor.close()
        self.logger.info(f"执行sql语句{sql}")

    def auto_commit_true(self, sql):
        """
        执行无返回结果的查询，sql是否提交，取决于autocommit的参数
        :param sql:需要执行的sql语句
        :return:
        """
        # 创建游标
        cursor = self.conn.cursor()
        cursor.execute(sql)
        cursor.close()
        #self.logger.info(f"执行sql语句{sql}")

    def close_conn(self):
        """
        关闭数据库连接
        :return: 无返回值
        """
        if self.conn:
            self.conn.close()
        self.logger.info("关闭数据库连接成功")

    def select_db(self, db):
        """
        选择使用的数据库，相当于use
        :param sql_name: 数据库名
        :return: 无返回
        """
        self.conn.select_db(db)
        self.logger.debug(f"切换数据库{db}")

    def query(self, sql):
        """
        执行一个有结果返回的sql
        :param sql: 需要执行的sql语句
        :return: 返回执行后的结果
        """
        # 创建游标
        cursor = self.conn.cursor()
        cursor.execute(f"{sql}")
        result = cursor.fetchall()
        cursor.close()
        self.logger.info(f"执行查询语句{sql}, 查询结果{result}")
        return result

    def check_table_excite(self, db_name, table):
        """
        此方法查询数据库表是否存在，存在返回True,不存在返回Flase
        :param db_name: 数据库名称
        :param table: 表名称
        :return: True/Flase
        """
        self.select_db(db_name)
        table_tuple = self.query("SHOW TABLES;")
        self.logger.info(f"{db_name}中的表有{table_tuple}")
        self.logger.info(f"确认查询{table}表是否存在{(table,) in table_tuple}")
        return (table,) in table_tuple

    def create_table(self, db, table_name, table_column):
        """
        查询表是否存在，如果不存在，则创建表，如果存在则跳过
        :param db:
        :param table_name:
        :param table_column:
        :return:
        """
        if not self.check_table_excite(db, table_name):
            # 判断数据表是否存在，如果不存在
            cursor = self.conn.cursor()
            cursor.execute(f'CREATE TABLE {table_name}({table_column});')
            self.logger.debug(f"在{db}中创建数据表{table_name}")
            cursor.close()
        else:
            self.logger.info(f"{table_name}已存在，创建表操作进行跳过")


def get_table_data(db_obj, db_name=conf.metadata_db_name, table_name=conf.metadata_table_name, create_table_cols=conf.metadata_create_table_cols):
    """
    获取已经执行过的数据
    :param db_obj:数据库连接对象
    :param db_name:数据库名称
    :param table_name:表名称
    :param create_table_cols:创建表语句行
    :return:返回已执行过的数据元组
    """
    # 抽取mysql中metadata中表file_monitor的数据
    # 获取mysql链接
    # 选择数据库
    db_obj.select_db(db_name)
    # 建表file_monitor
    db_obj.create_table(
        db_name,
        table_name,
        create_table_cols
    )
    # 查询表数据
    result = db_obj.query("select processed_file from file_monitor;")
    logger = logging_util.init_log()
    logger.info(f"查询已执行的数据文件，查询结果{result}")
    # 将结果转成列表list方便操作(("D:\\index.txt",), ("D:\\index.txt",))
    # 定义空列表存储
    processed_list = []
    for file in result:
        processed_list.append(file[0])
    return processed_list


