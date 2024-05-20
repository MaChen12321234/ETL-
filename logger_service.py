# coding:utf8
"""
1. 读取文件夹中有哪些文件
2. 从MySQL的元数据库中查询哪些文件是处理过的
3. 1和2的结果进行对比，找出哪些文件没有被处理，可以用于本次采集
4. 读取文件的每一行
5. 将每一行转换成Model
6. 将model调用生成sql语句，插入mysql
7. 将model调用to_csv写出csv
8. 记录元数据，本次哪些文件被处理了
9. 结束
"""
from util import file_service_util as file
from util import logging_util
from config import etl_config as conf
from util import mysql_util
from model import logger_service_model
# 得到日志对象
logger = logging_util.init_log()
logger.info('json数据开始处理。。。程序开始执行')
# TODO 步骤1读取文件夹中有哪些文件
get_all_file = file.file_service_util_list(path=conf.logger_service_file_path)
logger.info(f"读取用户提供路径下的文件夹：{get_all_file}")

# TODO 步骤2从元数据库中查询那些文件是已经处理过的
# 获取数据库连接对象
metadata_obj = mysql_util.MysqlUtil()
target_obj = mysql_util.MysqlUtil(
    host=conf.target_host,
    user=conf.target_user,
    password=conf.target_password,
    port=conf.target_port
)
# 查询数据库中的数据表是否存在，如果没有则进行创建
metadata_obj.create_table(
    conf.metadata_db_name,
    conf.metadata_logger_table_name,
    conf.metadata_logger_table_cols
)
target_obj.create_table(
    conf.target_db_name,
    conf.target_logger_service_table_name,
    conf.target_logger_service_table_cols
)
metadata_obj.select_db(conf.metadata_db_name)
# 查询元数据库中的已执行文件
processed_sql = f"SELECT processed_file FROM {conf.metadata_logger_table_name};"
# 执行sql 结果((1,), (2,), (3,))
processed_file = metadata_obj.query(processed_sql)
logger.info(f"获取已执行过的文件{processed_file}")

# TODO 步骤3 比对查询，找出哪些文件没有被处理，可以用于本次采集
# 定义一个列表存放已执行过的文件
processed_file_list = []
# 通过循环查询结果，将每一个数据存入列表
if processed_file:
    # 进入此处，则数据不为空
    for alone_file in processed_file:
        processed_file_list.append(alone_file[0])

# 开始比对
need_execute_file = file.get_need_file_data(get_all_file, processed_file_list)
logger.info(f"得到为执行过文件：{need_execute_file}")
# 定义一个列表存储数据模型对象
logger_service_model_list = []
single_file_count = dict()
# 读取文件
# TODO 步骤4 读取文件的每一行
for single_file in need_execute_file:
    # 记录文件数据行数。
    count_logger = 0
    for lines in open(single_file, "r", encoding="utf-8"):
        count_logger += 1
        lines = lines.replace("\n", "")
        lines = lines.replace("响应时间:", "")
        line = lines.split("\t")
        # TODO 步骤五 每一行转换成model
        logger_service_model_list.append(logger_service_model.LoggerServiceModel(line))

    # 将数据插入目标数据库
    # TODO 步骤六 将model调用生成sql语句，插入mysql
    count_sql = 0
    for alone_data in logger_service_model_list:
        query_insert_sql = alone_data.generate_insert_sql()
        target_obj.select_db(conf.target_db_name)
        target_obj.auto_commit_true(query_insert_sql)
        count_sql += 1
        # 每一千行提交一次数据
        if count_sql % 1000 == 0:
            target_obj.conn.commit()
            logger.info(f"已完成{count_sql}条数据插入")
    target_obj.conn.commit()
    single_file_count[single_file] = count_logger

    # TODO 步骤7 将model调用to_csv写出csv
    write_csv_f = open(conf.logger_date_path + conf.logger_output_csv_order_model_filename, "a", encoding="utf-8")
    count_csv = 0
    for alone_data_csv in logger_service_model_list:
        query_insert_csv = alone_data_csv.to_csv()
        count_csv += 1
        write_csv_f.write(query_insert_csv)
        # 没一千行提交一次
        if count_csv % 1000 == 0:
            write_csv_f.flush()
            logger.info(f"已完成{count_csv}条数据备份")
    write_csv_f.close()


# 写入元数据库
# 遍历需要执行的文件列表插入元数据库
# TODO 步骤8 记录元数据，本次哪些文件被处理了
for processed_al in need_execute_file:
    metadata_obj.select_db(conf.metadata_db_name)
    count = single_file_count[processed_al]
    sql = f"INSERT INTO {conf.metadata_logger_table_name}(processed_file, processed_line) VALUES (" \
          f"'{processed_al}', " \
          f"{count})"
    metadata_obj.auto_commit_false(sql)

# 关闭数据库连接
metadata_obj.close_conn()
target_obj.close_conn()