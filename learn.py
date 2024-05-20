# coding:utf8
"""
需求3：主业务逻辑文件，采集后台日志
"""
import time
from util import file_util as fu
from util.mysql_util import MySQLUtil, get_processed_files
from config import project_config as conf
from model.backend_logs_model import BackendLogsModel

# TODO 步骤1：读取文件夹中有哪些文件
# 路径请自行改到配置文件中
files = fu.get_dir_files_list("E:/pyetl-data-logs/backend_logs")

# TODO 步骤2： 读取mysql元数据库
# 构建元数据库连接db_util对象
metadata_db_util = MySQLUtil()

# 构建目标库的db_util对象
target_db_util = MySQLUtil(
    conf.target_host,
    user=conf.target_user,
    password=conf.target_password,
    port=conf.target_port,
    charset=conf.mysql_charset,
    autocommit=False
)
# 检查要写入数据的目标表是否存在，不存在创建
# 表名和建表列信息，自行改到配置文件中
target_db_util.check_table_exists_and_create(
    conf.target_db_name,
    "backend_logs",
    create_cols= \
        f"id int PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID', " \
        f"log_time TIMESTAMP(6) COMMENT '日志时间,精确到6位毫秒值', " \
        f"log_level VARCHAR(10) COMMENT '日志级别', " \
        f"log_module VARCHAR(50) COMMENT '输出日志的功能模块名', " \
        f"response_time INT COMMENT '接口响应时间毫秒', " \
        f"province VARCHAR(30) COMMENT '访问者省份', " \
        f"city VARCHAR(30) COMMENT '访问者城市', " \
        f"log_text VARCHAR(255) COMMENT '日志正文', " \
        f"INDEX(log_time)"
)


# 检查元数据的表是否存在
# 表名和建表的列信息，请自行改到配置文件中
# 工具方法获得MySQL中有哪些数据被处理了
# get_processed_files方法 会自动创建表，如果不存在
processed_files = get_processed_files(
    db_util=metadata_db_util,
    db_name=conf.metadata_db_name,
    table_name="backend_logs_monitor",
    create_cols="id INT PRIMARY KEY AUTO_INCREMENT, " \
    "file_name VARCHAR(255) NOT NULL COMMENT '处理文件名称', " \
    "process_lines INT NULL COMMENT '文件处理行数', " \
    "process_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '文件处理时间'"
)

# TODO 步骤3：对比找出哪些文件可以被处理
# need需要 process 处理
need_to_process_files = fu.get_new_by_compare_lists(processed_files, files)

# 构建一个文件对象，用于写出CSV
# 写出的路径请自行更改到配置文件中
backend_logs_write_f = open(
    "E:/pyetl-data-logs/output/csv/" + f'backend-logs-{time.strftime("%Y%m%d-%H%M%S", time.localtime(time.time()))}.csv',
    "a",
    encoding="UTF-8"
)

# 全局计数器
global_count = 0
# 构建一个字典，记录每一个文件被处理的行数
processed_files_record_dict = {}
# TODO 步骤4： 读取文件每一行进行处理
for file in need_to_process_files:
    single_file_count = 0       # 针对每一个文件被处理的行数的计数器
    for line in open(file, "r", encoding="UTF-8"):
        line = line.replace("\n", "")   # 处理掉每一行的\n回车符
        # TODO 步骤5：构建模型
        model = BackendLogsModel(line)

        # TODO 步骤6：写入MySQL（不要忘记构建目标库的db_util对象，以及检查目标表是否存在，不存在创建，参见19~42行）
        insert_sql = model.generate_insert_sql()
        target_db_util.select_db(conf.target_db_name)
        target_db_util.execute_without_autocommit(insert_sql)

        # TODO 步骤7：写出CSV（不要忘记构建写出CSV的文件对象）
        backend_logs_write_f.write(model.to_csv())
        backend_logs_write_f.write("\n")

        global_count += 1
        single_file_count += 1
        if global_count % 1000 == 0:
            # 提交MySQL
            target_db_util.conn.commit()
            # 刷新文件写出的缓冲区
            backend_logs_write_f.flush()

    processed_files_record_dict[file] = single_file_count


target_db_util.conn.commit()
backend_logs_write_f.close()

# TODO 步骤8：记录元数据
for file_name in processed_files_record_dict.keys():
    count = processed_files_record_dict[file_name]
    sql = f"INSERT INTO backend_logs_monitor(file_name, process_lines) VALUES(" \
          f"'{file_name}', {count}" \
          f")"
    metadata_db_util.select_db(conf.metadata_db_name)
    metadata_db_util.execute(sql)

metadata_db_util.close_conn()
target_db_util.close_conn()