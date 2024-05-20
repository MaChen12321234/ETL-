import datetime
from unittest import TestCase

import config.etl_config
from util import mysql_util
from util.mysql_util import get_table_data


class TestMysqlUtil(TestCase):
    def setUp(self) -> None:
        self.conn = mysql_util.MysqlUtil()

    def test_auto_commit_false(self):
        """
        不管什么参数都会直接提交
        :return:
        """
        # 设置autocommit为True
        self.conn.conn.autocommit(True)
        self.conn.select_db("book")
        self.conn.create_table(
            "book",
            "auto_commit_false",
            "id int primary key, name varchar(30)"
        )
        self.conn.auto_commit_true(f'insert into auto_commit_false value (1, "李华");')

        # 设置autocommit为Flase
        self.conn.conn.autocommit(False)
        self.conn.auto_commit_false(f'insert into auto_commit_false value (2, "小明");')
        result = self.conn.query("select * from auto_commit_false;")
        self.conn.auto_commit_false("truncate table auto_commit_false;")
        self.assertEqual(((1, "李华"), (2, "小明")), result)

    def test_auto_commit_true(self):
        """
        如果参数是True则提交，不上传则不提交
        :return:
        """
        # 设置autocommit为True
        self.new_sql_obj = mysql_util.MysqlUtil()
        self.new_sql_obj.select_db("book")
        self.new_sql_obj.conn.autocommit(True)
        self.new_sql_obj.create_table(
            "book",
            "auto_commit_true",
            "id int primary key, name varchar(30)"
        )
        self.new_sql_obj.auto_commit_true(f'insert into auto_commit_true values (1, "李华");')
        self.new_sql_obj.close_conn()

        # 设置autocommit为False
        self.new_sql = mysql_util.MysqlUtil()
        self.new_sql.select_db("book")
        self.new_sql.conn.autocommit(False)
        self.new_sql.auto_commit_true(f'insert into auto_commit_true values (2, "小明");')
        self.new_sql.close_conn()
        self.new_sql2 = mysql_util.MysqlUtil()
        self.new_sql2.select_db("book")
        result = self.new_sql2.query("select * from auto_commit_true;")
        self.new_sql2.auto_commit_false("truncate table auto_commit_true;")
        self.assertEqual(((1, "李华"),), result)

    def test_query(self):
        self.conn.select_db("book")
        self.conn.create_table(
            "book",
            "query",
            "id int primary key, name varchar(30)"
        )
        self.conn.auto_commit_false(f'insert into query values (1, "李华"), (2, "小明")')
        result = self.conn.query("select * from query;")
        self.conn.auto_commit_false("truncate table query;")
        self.assertEqual(((1, "李华"), (2, "小明")), result)

    def test_select_db(self):
        self.conn.select_db("book")
        result = self.conn.query("select database()")
        self.assertEqual((("book",), ), result)

    def test_check_table_excite(self):
        self.conn.select_db("book")
        self.conn.create_table(
            "book",
            "check_table_excite",
            "id int primary key, name varchar(30)"
        )
        result = self.conn.check_table_excite("book", "check_table_excite")
        self.conn.auto_commit_false("drop table check_table_excite")
        self.assertEqual(True, result)

    def test_create_table(self):
        self.conn.select_db("book")
        self.conn.create_table(
            "book",
            "create_table",
            "id int primary key, name varchar(30)")
        result = self.conn.check_table_excite("book", "create_table")
        self.conn.auto_commit_false("drop table create_table;")
        self.assertEqual(True, result)

    def test_get_table_data(self):
        self.new_conn_obj = mysql_util.MysqlUtil()
        get_table_data(self.conn, db_name="book")    # 创建表，目前没有数据
        # 选择数据库
        self.conn.select_db("book")
        # 插入数据
        self.conn.auto_commit_false('insert into file_monitor values (1, "D:\\\index.txt", 1024, "2024-05-10 14:24:17");')
        # 测试方法，观察是否有数据返回与预期结果进行比对
        select_t = get_table_data(self.conn, db_name="book")
        # 清空测试残余
        self.conn.auto_commit_false("truncate table file_monitor;")
        self.assertEqual(["D:\\index.txt"], select_t)
