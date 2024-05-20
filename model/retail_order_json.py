# coding:utf8
import json
from util import time_util, str_util
from config import etl_config as conf


class RetailOrderJson(object):
    """
    构建订单模型
    """

    # 属性
    def __init__(self, data_a: str):
        """
        传入数据
        :param data_a:json数据按行传入
        """
        # 将数据转为字典类型存储
        data = json.loads(data_a)
        self.discount_rate = data['discountRate']  # 折扣率
        self.store_shop_no = data['storeShopNo']  # 店铺店号（无用列）
        self.day_order_seq = data['dayOrderSeq']  # 本单为当日第几单
        self.store_district = data['storeDistrict']  # 店铺所在行政区
        self.is_signed = data['isSigned']  # 是否签约店铺（签约第三方支付体系）
        self.store_province = data['storeProvince']  # 店铺所在省份
        self.origin = data['origin']  # 原始信息（无用）
        self.store_gps_longitude = data['storeGPSLongitude']  # 店铺GPS经度
        self.discount = data['discount']  # 折扣金额
        self.store_id = data['storeID']  # 店铺ID
        self.product_count = data['productCount']  # 本单售卖商品数量
        self.operator_name = data['operatorName']  # 操作员姓名
        self.operator = data['operator']  # 操作员ID
        self.store_status = data['storeStatus']  # 店铺状态
        self.store_own_user_tel = data['storeOwnUserTel']  # 店铺店主电话
        self.pay_type = data['payType']  # 支付类型
        self.discount_type = data['discountType']  # 折扣类型
        self.store_name = data['storeName']  # 店铺名称
        self.store_own_user_name = data['storeOwnUserName']  # 店铺店主名称
        self.date_ts = data['dateTS']  # 订单时间
        self.small_change = data['smallChange']  # 找零金额
        self.store_gps_name = data['storeGPSName']  # 店铺GPS名称
        self.erase = data['erase']  # 是否抹零
        self.store_gps_address = data['storeGPSAddress']  # 店铺GPS地址
        self.order_id = data['orderID']  # 订单ID
        self.money_before_whole_discount = data['moneyBeforeWholeDiscount']  # 折扣前金额
        self.store_category = data['storeCategory']  # 店铺类别
        self.receivable = data['receivable']  # 应收金额
        self.face_id = data['faceID']  # 面部识别ID
        self.store_own_user_id = data['storeOwnUserId']  # 店铺店主ID
        self.payment_channel = data['paymentChannel']  # 付款通道
        self.payment_scenarios = data['paymentScenarios']  # 付款情况（无用）
        self.store_address = data['storeAddress']  # 店铺地址
        self.total_no_discount = data['totalNoDiscount']  # 整体价格（无折扣）
        self.payed_total = data['payedTotal']  # 已付款金额
        self.store_gps_latitude = data['storeGPSLatitude']  # 店铺GPS纬度
        self.store_create_date_ts = data['storeCreateDateTS']  # 店铺创建时间
        self.store_city = data['storeCity']  # 店铺所在城市
        self.member_id = data['memberID']  # 会员ID

    def check_undefined_to_rename(self):
        """
        将无意义的参数替换
        :return:
        """
        if str_util.check_str_undefined(self.store_province):
            self.store_province = "未知省份"
        if str_util.check_str_undefined(self.store_city):
            self.store_city = "未知城市"
        if str_util.check_str_undefined(self.store_district):
            self.store_district = "未知地区"

    # 方法
    def to_csv(self, sep=","):
        self.check_undefined_to_rename()
        csv_line = \
            f"{self.order_id}{sep}" \
            f"{self.store_id}{sep}" \
            f"{self.store_name}{sep}" \
            f"{self.store_status}{sep}" \
            f"{self.store_own_user_id}{sep}" \
            f"{self.store_own_user_name}{sep}" \
            f"{self.store_own_user_tel}{sep}" \
            f"{self.store_category}{sep}" \
            f"{self.store_address}{sep}" \
            f"{self.store_shop_no}{sep}" \
            f"{self.store_province}{sep}" \
            f"{self.store_city}{sep}" \
            f"{self.store_district}{sep}" \
            f"{self.store_gps_name}{sep}" \
            f"{self.store_gps_address}{sep}" \
            f"{self.store_gps_longitude}{sep}" \
            f"{self.store_gps_latitude}{sep}" \
            f"{self.is_signed}{sep}" \
            f"{self.operator}{sep}" \
            f"{self.operator_name}{sep}" \
            f"{self.face_id}{sep}" \
            f"{self.member_id}{sep}" \
            f"{time_util.GetTimeStamp.get_date(self.store_create_date_ts)}{sep}" \
            f"{self.origin}{sep}" \
            f"{self.day_order_seq}{sep}" \
            f"{self.discount_rate}{sep}" \
            f"{self.discount_type}{sep}" \
            f"{self.discount}{sep}" \
            f"{self.money_before_whole_discount}{sep}" \
            f"{self.receivable}{sep}" \
            f"{self.erase}{sep}" \
            f"{self.small_change}{sep}" \
            f"{self.total_no_discount}{sep}" \
            f"{self.payed_total}{sep}" \
            f"{self.pay_type}{sep}" \
            f"{self.payment_channel}{sep}" \
            f"{self.payment_scenarios}{sep}" \
            f"{self.product_count}{sep}" \
            f"{time_util.GetTimeStamp.get_date(self.date_ts)}"
        return csv_line

    def generate_insert_sql(self):
        """
        将传入的json数据转换成sql语句方便后续操作
        :return: 返回sql
        """
        sql = f"INSERT IGNORE INTO {conf.target_table_name}(" \
              f"order_id,store_id,store_name,store_status,store_own_user_id," \
              f"store_own_user_name,store_own_user_tel,store_category," \
              f"store_address,store_shop_no,store_province,store_city," \
              f"store_district,store_gps_name,store_gps_address," \
              f"store_gps_longitude,store_gps_latitude,is_signed," \
              f"operator,operator_name,face_id,member_id,store_create_date_ts," \
              f"origin,day_order_seq,discount_rate,discount_type,discount," \
              f"money_before_whole_discount,receivable,erase,small_change," \
              f"total_no_discount,pay_total,pay_type,payment_channel," \
              f"payment_scenarios,product_count,date_ts" \
              f") VALUES(" \
              f"'{self.order_id}', " \
              f"{self.store_id}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_name)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_status)}, " \
              f"{self.store_own_user_id}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_own_user_name)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_own_user_tel)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_category)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_address)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_shop_no)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_province)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_city)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_district)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_gps_name)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_gps_address)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_gps_longitude)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.store_gps_latitude)}, " \
              f"{self.is_signed}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.operator)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.operator_name)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.face_id)}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.member_id)}, " \
              f"'{time_util.GetTimeStamp.get_date(self.store_create_date_ts)}', " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.origin)}, " \
              f"{self.day_order_seq}, " \
              f"{self.discount_rate}, " \
              f"{self.discount_type}, " \
              f"{self.discount}, " \
              f"{self.money_before_whole_discount}, " \
              f"{self.receivable}, " \
              f"{self.erase}, " \
              f"{self.small_change}, " \
              f"{self.total_no_discount}, " \
              f"{self.payed_total}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.pay_type)}, " \
              f"{self.payment_channel}, " \
              f"{str_util.check_str_null_and_transform_to_sql_null(self.payment_scenarios)}, " \
              f"{self.product_count}, " \
              f"'{time_util.GetTimeStamp.get_date(self.date_ts)}')"
        return sql


class OrdersDetailModel:
    """此方法记录订单详情表，订单号+商品"""

    def __init__(self, data):
        # 传入字符串，将利用api将json文件转换成字典形式
        data_dict = json.loads(data)
        # 以订单号加商品作为标识
        self.order_id = data_dict["orderID"]
        # 定义一个列表容器，存储product
        product_detail = data_dict["product"]
        # 定义一个空容器，用于存放product里面的每一个商品，重新定义一个类，
        # 用于存放作为每一个商品属性的模型，通过for循环，创建每一个订单商品对象
        # 并放到次空容器中存储
        self.product_list = []
        for alone in product_detail:
            self.product_list.append(SingleProductSoldModel(self.order_id, alone))

    def generate_insert_sql(self):
        """此方法用于对模型中记录的数据生成sql语句"""
        # 通过for循环，将product_list中的每一个商品都生成一个sql
        sql = f"INSERT IGNORE INTO {conf.target_orders_detail_table_name}(" \
              f"order_id,barcode,name,count,price_per,retail_price,trade_price,category_id,unit_id) VALUES"
        # 循环添加语句
        for model in self.product_list:
            sql += "("
            sql += f"'{model.order_id}', " \
                   f"{str_util.check_str_null_and_transform_to_sql_null(model.barcode)}, " \
                   f"{str_util.check_str_null_and_transform_to_sql_null(model.name)}, " \
                   f"{model.count}, " \
                   f"{model.price_per}, " \
                   f"{model.retail_price}, " \
                   f"{model.trade_price}, " \
                   f"{model.category_id}, " \
                   f"{model.unit_id}"
            sql += "), "
        sql = sql[:-2]
        return sql


class SingleProductSoldModel:
    """此模型存放订单中的每个商品信息"""
    def __init__(self, order_id, product_detail_dict):
        self.order_id = order_id                                    # 订单ID
        self.name = product_detail_dict["name"]                     # 商品名称
        self.count = product_detail_dict["count"]                   # 商品售卖数量
        self.unit_id = product_detail_dict["unitID"]                # 单位ID
        self.barcode = product_detail_dict["barcode"]               # 商品的条码
        self.price_per = product_detail_dict["pricePer"]            # 商品卖出的单价
        self.retail_price = product_detail_dict["retailPrice"]      # 商品建议零售价
        self.trade_price = product_detail_dict["tradePrice"]        # 商品建议成本价
        self.category_id = product_detail_dict["categoryID"]

    def to_csv(self, sep=","):
        csv_line = \
            f"{self.order_id}{sep}" \
            f"{self.barcode}{sep}" \
            f"{self.name}{sep}" \
            f"{self.count}{sep}" \
            f"{self.price_per}{sep}" \
            f"{self.retail_price}{sep}" \
            f"{self.trade_price}{sep}" \
            f"{self.category_id}{sep}" \
            f"{self.unit_id}"
        return csv_line

