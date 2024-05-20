"""
该文件为etl项目的所有配置项
"""
# ##########################################3 日志配置想如下

# 1、路径配置
import time

log_path = 'D:\PycharmProjects\pythonProjectETL/logs/'
# 2、文件名配置\
log_name = time.strftime("%Y%m%d", time.localtime(time.time())) + '.log'
log_file = log_path + log_name

# #############################################日志配置结束


# #############################################json订单数据采集配置项开始
# 被采集json数据在那个文件夹
json_date_path = "D:/Python/ETL/json"
json_output_csv_order_model_path = "D:/Python/ETL/csv/"
json_output_csv_order_model_filename = "order-" + time.strftime("%Y%m%d", time.localtime(time.time())) + ".csv"
json_output_csv_order_detail_filename = "detail-" + time.strftime("%Y%m%d", time.localtime(time.time())) + ".csv"
# #############################################json订单数据采集配置项结束

# #############################################数据库配置文件开始
charset = "utf8"
# 元数据库配置
metadata_host = "localhost"
metadata_user = "root"
metadata_password = 'root'
metadata_port = 3306
metadata_db_name = "metadata"
metadata_table_name = "file_monitor"
metadata_create_table_cols = """
        id int auto_increment primary key comment '主键ID', 
        processed_file varchar(255) unique comment "已处理文件",
        processed_line int comment "处理数据条数",
        processed_time datetime default current_timestamp()
"""
metadata_barcode_table_name = "barcode_monitor"
metadata_barcode_table_cols = "id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID', " \
                                             "time_record TIMESTAMP NOT NULL COMMENT '本次采集记录的最大时间', " \
                                             "gather_line_count INT NULL COMMENT '本次采集条数'"
metadata_logger_table_name = "logger_monitor"
metadata_logger_table_cols = """
        id int auto_increment primary key comment '主键ID', 
        processed_file varchar(255) unique comment "已处理文件",
        processed_line int comment "处理数据条数",
        processed_time datetime default current_timestamp()
"""
# 源数据库配置
source_host = metadata_host
source_user = metadata_user
source_password = metadata_password
source_port = metadata_port
source_db_name = "source_data"
source_table_name = "sys_barcode"

# 目标数据库target_barcode数据库配置
target_barcode_table_name = "barcode"
target_barcode_table_cols = """
    `code` varchar(50) PRIMARY KEY COMMENT '商品条码',
  `name` varchar(200) DEFAULT '' COMMENT '商品名称',
  `spec` varchar(200) DEFAULT '' COMMENT '商品规格',
  `trademark` varchar(100) DEFAULT '' COMMENT '商品商标',
  `addr` varchar(200) DEFAULT '' COMMENT '商品产地',
  `units` varchar(50) DEFAULT '' COMMENT '商品单位(个、杯、箱、等)',
  `factory_name` varchar(200) DEFAULT '' COMMENT '生产厂家',
  `trade_price` DECIMAL(50, 5) DEFAULT 0.0 COMMENT '贸易价格(指导进价)',
  `retail_price` DECIMAL(50, 5) DEFAULT 0.0 COMMENT '零售价格(建议卖价)',
  `update_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `wholeunit` varchar(50) DEFAULT NULL COMMENT '大包装单位',
  `wholenum` int(11) DEFAULT NULL COMMENT '大包装内装数量',
  `img` varchar(500) DEFAULT NULL COMMENT '商品图片',
  `src` varchar(20) DEFAULT NULL COMMENT '源信息', 
  INDEX (update_at)
"""
# logger业务目标数据库相关配置
target_logger_service_table_name = "logger_service"
target_logger_service_table_cols = """
    `id` int PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    `logger_time` varchar(200) DEFAULT '' COMMENT '日志时间',
    `logger_level` varchar(200) DEFAULT '' COMMENT '日志等级',
    `code_module` varchar(200) DEFAULT '' COMMENT '代码模块',
    `response_time` varchar(100) DEFAULT '' COMMENT '响应时间',
    `province` varchar(200) DEFAULT '' COMMENT '省份',
    `city` varchar(50) DEFAULT '' COMMENT '城市',
    `logger_manager` varchar(200) DEFAULT '' COMMENT '日志内容'
"""
# 目标数据库相关配置
target_host = metadata_host
target_user = metadata_user
target_password = metadata_password
target_port = metadata_port
target_db_name = "retail"
target_table_name = "orders"
target_create_table_cols = \
    f"order_id VARCHAR(255) PRIMARY KEY, " \
    f"store_id INT COMMENT '店铺ID', " \
    f"store_name VARCHAR(30) COMMENT '店铺名称', " \
    f"store_status VARCHAR(10) COMMENT '店铺状态(open,close)', " \
    f"store_own_user_id INT COMMENT '店主id', " \
    f"store_own_user_name VARCHAR(50) COMMENT '店主名称', " \
    f"store_own_user_tel VARCHAR(15) COMMENT '店主手机号', " \
    f"store_category VARCHAR(10) COMMENT '店铺类型(normal,test)', " \
    f"store_address VARCHAR(255) COMMENT '店铺地址', " \
    f"store_shop_no VARCHAR(255) COMMENT '店铺第三方支付id号', " \
    f"store_province VARCHAR(10) COMMENT '店铺所在省', " \
    f"store_city VARCHAR(10) COMMENT '店铺所在市', " \
    f"store_district VARCHAR(10) COMMENT '店铺所在行政区', " \
    f"store_gps_name VARCHAR(255) COMMENT '店铺gps名称', " \
    f"store_gps_address VARCHAR(255) COMMENT '店铺gps地址', " \
    f"store_gps_longitude VARCHAR(255) COMMENT '店铺gps经度', " \
    f"store_gps_latitude VARCHAR(255) COMMENT '店铺gps纬度', " \
    f"is_signed TINYINT COMMENT '是否第三方支付签约(0,1)', " \
    f"operator VARCHAR(10) COMMENT '操作员', " \
    f"operator_name VARCHAR(50) COMMENT '操作员名称', " \
    f"face_id VARCHAR(255) COMMENT '顾客面部识别ID', " \
    f"member_id VARCHAR(255) COMMENT '顾客会员ID', " \
    f"store_create_date_ts TIMESTAMP COMMENT '店铺创建时间', " \
    f"origin VARCHAR(255) COMMENT '原始信息(无用)', " \
    f"day_order_seq INT COMMENT '本订单是当日第几单', " \
    f"discount_rate DECIMAL(10, 5) COMMENT '折扣率', " \
    f"discount_type TINYINT COMMENT '折扣类型', " \
    f"discount DECIMAL(10, 5) COMMENT '折扣金额', " \
    f"money_before_whole_discount DECIMAL(10, 5) COMMENT '折扣前总金额', " \
    f"receivable DECIMAL(10, 5) COMMENT '应收金额', " \
    f"erase DECIMAL(10, 5) COMMENT '抹零金额', " \
    f"small_change DECIMAL(10, 5) COMMENT '找零金额', " \
    f"total_no_discount DECIMAL(10, 5) COMMENT '总价格(无折扣)', " \
    f"pay_total DECIMAL(10, 5) COMMENT '付款金额', " \
    f"pay_type VARCHAR(10) COMMENT '付款类型', " \
    f"payment_channel TINYINT COMMENT '付款通道', " \
    f"payment_scenarios VARCHAR(15) COMMENT '付款描述(无用)', " \
    f"product_count INT COMMENT '本单卖出多少商品', " \
    f"date_ts TIMESTAMP COMMENT '订单时间', " \
    f"INDEX (receivable), INDEX (date_ts)"

# JSON数据采集后，写入MySQL，存储订单详情（带商品信息的）相关的表，表名是：
target_orders_detail_table_name = "orders_detail"
# orders_detail表的建表语句的列信息
target_orders_detail_table_create_cols = \
    f"order_id VARCHAR(255) COMMENT '订单ID', " \
    f"barcode VARCHAR(255) COMMENT '商品条码', " \
    f"name VARCHAR(255) COMMENT '商品名称', " \
    f"count INT COMMENT '本单此商品卖出数量', " \
    f"price_per DECIMAL(10, 5) COMMENT '实际售卖单价', " \
    f"retail_price DECIMAL(10, 5) COMMENT '零售建议价', " \
    f"trade_price DECIMAL(10, 5) COMMENT '贸易价格(进货价)', " \
    f"category_id INT COMMENT '商品类别ID', " \
    f"unit_id INT COMMENT '商品单位ID(包、袋、箱、等)', " \
    f"PRIMARY KEY (order_id, barcode)"
# #############################################数据库配置文件结束


# barcode业务数据备份csv
barcode_date_path = "D:/Python/ETL/csv/barcode"
barcode_output_csv_order_model_filename = "barcode-" + time.strftime("%Y%m%d", time.localtime(time.time())) + ".csv"
# #############################################barcode业务数据备份csv

# logger业务数据备份csv
logger_date_path = "D:/Python/ETL/csv/logger"
logger_output_csv_order_model_filename = "logger-" + time.strftime("%Y%m%d", time.localtime(time.time())) + ".csv"
# #############################################logger业务数据备份csv


# #############################################logger业务配置开始
logger_service_file_path = "D:/PycharmProjects/pythonProjectETL/logs/logging"
# #############################################logger业务配置结束