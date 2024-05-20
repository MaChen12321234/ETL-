import sys

from util import logging_util
from util import mysql_util
from config import etl_config as conf
from model import barcode_model
"""
此代码可将源数据库数据写入自己数据库
"""

# 定义日志方法对象
logger = logging_util.init_log()
logger.info("采集mysql数据库程序启动。。。。")
# 建立三个数据库连接，分别为源数据库，元数据库，目标数据库
# 数据源数据库
source_mysql_conn = mysql_util.MysqlUtil(
    host=conf.source_host,
    user=conf.source_user,
    password=conf.source_password,
    port=conf.source_port
)
# 目标数据库
target_mysql_conn = mysql_util.MysqlUtil(
    host=conf.target_host,
    user=conf.target_user,
    password=conf.source_password,
    port=conf.source_port
)
# 元数据库
metadata_mysql_conn = mysql_util.MysqlUtil()

# 判断源数据库是否存在
if not source_mysql_conn.check_table_excite(conf.source_db_name, conf.source_table_name):
    logger.info("源数据库中没有对应表存在，请开启社交属性进行交流沟通。。。。")
    sys.exit(1)

# 判断目标数据库表是否建好，如果没有，则创建表
target_mysql_conn.select_db(conf.target_db_name)
target_mysql_conn.create_table(
    conf.target_db_name,
    conf.target_barcode_table_name,
    conf.target_barcode_table_cols
)

metadata_mysql_conn.select_db(conf.metadata_db_name)
update_data = None
# 判断元数据库表是否存在，如果不存在则创建表，新增变量存放更新日期，通过比对日期来转移数据
if not metadata_mysql_conn.check_table_excite(conf.metadata_db_name, conf.metadata_barcode_table_name):
    # 如果不存在则需要创建表
    metadata_mysql_conn.create_table(
        conf.metadata_db_name,
        conf.metadata_barcode_table_name,
        conf.metadata_barcode_table_cols
    )
else:
    # 若存在，则查询最后一次更新的时间
    query_sql = f"SELECT time_record FROM {conf.metadata_barcode_table_name} ORDER BY time_record DESC LIMIT 1;"
    result = metadata_mysql_conn.query(query_sql)
    # 查询到的结果不一定有数据，如果有数据，则去除第一个数据作为更新时间，如果没有，那就是none
    if len(result) != 0:
        # 代表有数据
        update_data = str(result[0][0])

source_mysql_conn.select_db(conf.source_db_name)
if update_data:
    # 表示有数据
    query_up_sql = f"SELECT * FROM {conf.source_table_name} WHERE updateAT >= {update_data}" \
                   f" ORDER BY updateAT;"
else:
    query_up_sql = f"SELECT * FROM {conf.source_table_name} ORDER BY updateAT;"

# 查询
query_result = source_mysql_conn.query(query_up_sql)

# 定义一个列表存储模型实例化后的对象
barcode_models = []
# 定义计数器
count_sql = 0
# 循环构建模型对象
for single_barcode in query_result:
    code = single_barcode[0]
    name = single_barcode[1]
    spec = single_barcode[2]
    trademark = single_barcode[3]
    addr = single_barcode[4]
    units = single_barcode[5]
    factory_name = single_barcode[6]
    trade_price = single_barcode[7]
    retail_price = single_barcode[8]
    update_at = str(single_barcode[9])  # single_line_result[9]是读取的updateAt时间，类型是datetime，转换成字符串
    wholeunit = single_barcode[10]
    wholenum = single_barcode[11]
    img = single_barcode[12]
    src = single_barcode[13]
    model = barcode_model.BarcodeModel(
        code=code,
        name=name,
        spec=spec,
        trademark=trademark,
        addr=addr,
        units=units,
        factory_name=factory_name,
        trade_price=trade_price,
        retail_price=retail_price,
        update_at=update_at,
        wholeunit=wholeunit,
        wholenum=wholenum,
        img=img,
        src=src
    )
    barcode_models.append(model)
# 支持，我们得到了模型化对象，存储于列表中
# 通过循环插入数据库，完成需求
target_mysql_conn.select_db(conf.target_db_name)
max_last_update_time = "2000-01-01 00:00:00"
for model in barcode_models:
    # 得到每一个对象
    # 取出更新时间的属性，用于比较
    current_data_time = model.update_at
    # 比较，如果得到的时间，大于最大时间，则将该事件赋值给max_last_update_time
    if current_data_time > max_last_update_time:
        max_last_update_time = current_data_time
    # 插入目标数据库
    sql = model.generate_insert_sql()
    target_mysql_conn.auto_commit_true(sql)

    # 一千条提交一次
    count_sql += 1
    if count_sql % 1000 == 0:
        target_mysql_conn.conn.commit()
        logger.info(f"从数据源：{conf.source_db_name}库，读取表：{conf.source_table_name}，"
                    f"当前写入目标表：{conf.target_barcode_table_name}完成，最终写入：{count_sql}行")
target_mysql_conn.conn.commit()
logger.info(f"从数据源：{conf.source_db_name}库，读取表：{conf.source_table_name}，"
            f"当前写入目标表：{conf.target_barcode_table_name}完成，最终写入：{count_sql}行")
barcode_write_csv = open(
    conf.barcode_date_path + conf.barcode_output_csv_order_model_filename,
    "a",
    encoding="utf-8"
)
count_csv = 0
for model in barcode_models:
    csv_line = model.to_csv()
    barcode_write_csv.write(csv_line)
    barcode_write_csv.write("\n")
    count_csv += 1
    if count_csv % 1000 == 0:
        barcode_write_csv.flush()
        logger.info(f"从数据源：{conf.source_db_name}库，读取表：{conf.source_table_name}，"
                    f"写出CSV到：{barcode_write_csv.name}， 当前写出：{count_csv}行。")
barcode_write_csv.close()
logger.info(f"从数据源：{conf.source_db_name}库，读取表：{conf.source_table_name}，"
            f"写出CSV到：{barcode_write_csv.name} 完成， 最终写出：{count_csv}行。")
# 记录表转移数据的时间
metadata_mysql_conn.select_db(conf.metadata_db_name)
sql_metadata = f"INSERT INTO {conf.metadata_barcode_table_name}(" \
      f"time_record, gather_line_count) VALUES(" \
      f"'{max_last_update_time}', " \
      f"{count_sql}" \
      f")"
print(sql_metadata)
metadata_mysql_conn.auto_commit_false(sql_metadata)

metadata_mysql_conn.conn.close()
target_mysql_conn.conn.close()
source_mysql_conn.conn.close()

logger.info("读取MySQL数据，写入目标MySQL和CSV程序执行完成。。。。。。")
