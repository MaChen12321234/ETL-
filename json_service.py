'''
采集json数据到mysql或者csv文件
'''
from util.logging_util import init_log
from util import file_service_util as fsu
from config import etl_config as conf
from util import mysql_util
from model import retail_order_json as order_js

logger = init_log()
logger.info('json数据开始处理。。。程序开始执行')

file_bat = fsu.file_service_util_list(path=conf.json_date_path, recursion=True)

# 判断那些文件是可执行的，那些文件是不可执行的
# 通过代码方式获取mysql中的数据，与files进行比对

# 抽取mysql中metadata中表file_monitor的数据
# 获取元mysql链接
metadata_get_db_obj = mysql_util.MysqlUtil()
# 获取目标数据库连接
target_get_db_obj = mysql_util.MysqlUtil(conf.target_host, conf.target_user, conf.target_password, conf.target_port,
                                         autocommit=False, charset=conf.charset)
pass_file = mysql_util.get_table_data(metadata_get_db_obj)

# 调用对比文件方法，来获取需要执行的数据文件
get_need_data = fsu.get_need_file_data(file_bat, pass_file)
# 定义空列表，存储所有的订单对象
order_model_list = []
# 定义空列表，存储所有的订单详情对象
order_detail_list = []
# 定义一个字典，以键值对的形式记录已处理文件和行数
processed_file_name_and_line = dict()
globe_count = 0
# 从获取的数据文件中读取需要执行的文件路径挨个进行遍历存储至数据模型类中
for file_alone in get_need_data:
    # 记录数据处理条数

    # 记录每个文件已处理的行数
    processed_file_line = 0
    for lines in open(file_alone, "r", encoding="utf-8"):
        globe_count += 1
        processed_file_line += 1
        line = lines.replace("\n", "")
        order_model = order_js.RetailOrderJson(line)
        order_detail = order_js.OrdersDetailModel(line)
        order_model_list.append(order_model)
        order_detail_list.append(order_detail)

    # 数据过滤，遍历过滤掉所有的脏数据,定义个列表，存储过滤后的数据
    reserved_models = []
    for model in order_model_list:
        if model.receivable <= 10000:
            reserved_models.append(model)

    # 写入csv
    order_model_write = open(conf.json_output_csv_order_model_path + conf.json_output_csv_order_model_filename, "a",
                             encoding="utf-8")
    order_detail_write = open(conf.json_output_csv_order_model_path + conf.json_output_csv_order_detail_filename, "a",
                              encoding="utf-8")
    for model in reserved_models:
        csv_line = model.to_csv(sep=",")
        order_model_write.write(csv_line)
        order_model_write.write("\n")
    order_model_write.close()

    for model in order_detail_list:
        for single in model.product_list:
            csv_line = single.to_csv(sep=",")
            order_detail_write.write(csv_line)
            order_detail_write.write("\n")
    order_model_write.close()
    logger.info(f"完成了CSV备份文件的写出，写出到了：{conf.json_output_csv_order_model_path}")

    # 将订单数据插入数据库中
    # 创建订单数据表
    target_get_db_obj.select_db(conf.target_db_name)
    target_get_db_obj.create_table(conf.target_db_name, conf.target_table_name,
                                   conf.target_create_table_cols)
    # 创建订单详情表
    target_get_db_obj.create_table(conf.target_db_name, conf.target_orders_detail_table_name,
                                   conf.target_orders_detail_table_create_cols)
    # 插入订单表数据
    for model in reserved_models:
        target_get_db_obj.select_db(conf.target_db_name)
        sql = model.generate_insert_sql()
        target_get_db_obj.auto_commit_true(sql)

    # 插入订单详情数据
    for detail_model in order_detail_list:
        target_get_db_obj.select_db(conf.target_db_name)
        sql_detail = detail_model.generate_insert_sql()
        # print(sql_detail)
        target_get_db_obj.auto_commit_true(sql_detail)
    processed_file_name_and_line[file_alone] = processed_file_line
# 提交
target_get_db_obj.conn.commit()

logger.info(f"完成了CSV备份文件的写出，写出到了：{conf.json_output_csv_order_model_path}")
logger.info(f"完成了向MySQL数据库中插入数据的操作。"
            f"共处理了：{globe_count}条数据")

# 循环遍历存储已处理字典，将所有键值对插入mysql
for processed_data_alone in processed_file_name_and_line.keys():
    processed_line = processed_file_name_and_line[processed_data_alone]
    insert_sql = f"INSERT INTO {conf.metadata_table_name}(processed_file, processed_line) " \
                 f"VALUES ('{processed_data_alone}', {processed_line});"
    metadata_get_db_obj.auto_commit_false(insert_sql)
metadata_get_db_obj.conn.close()
target_get_db_obj.conn.close()
logger.info("读取json数据，向mysql中插入以及向csv进行备份，程序完成。。。。")