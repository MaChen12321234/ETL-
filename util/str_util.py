# coding:utf8
"""
检查字符串是不是无意义的字符串，如果是无意义的，则返回True，否则返回False
"""


def check_str_undefined(data):
    """
    检查字符串是不是无意义的字符串
    :param data: 需要检查的字符串
    :return: 如果是无意义的，则返回True，否则返回False
    """
    if not data:
        return True
    # 将所有字符转换为小写
    data_change = data.lower()
    # 判断是否无意义
    if data_change == "none" or data_change == "null" or data_change == "undefined":
        return True
    return False


def check_str_null_and_transform_to_sql_null(data):
    """
    此方法用于对传入的字符串校验器是否有效，如果无效则返回sql意义上的NULL，
    如果有效，则返回‘字符本身’
    :param data: 传入的数据字符
    :return: 返回sql格式null或者本身
    """
    if check_str_undefined(str(data)):
        return "NULL"
    else:
        return f"'{data}'"


def check_number_null_and_transform_to_sql_null(data):
    """
    此方法将传入的数据数字查询是否有意义，如果有则返回原数字，如果没有，则返回sql中的NULL
    :param data:
    :return:
    """
    clean_str(data)
    if data and not check_str_undefined(str(data)):
        clean_str(data)
        return data
    else:
        return "NULL"


def clean_str(data):
    """
    如果得到的数据有特殊字符，则影响我们正常业务中的插入操作，此方法可对特殊字符进行清理
    :param data:需要清理的数据
    :return:返回干净的字符
    """
    if check_str_undefined(data):
        # 如果数据无意义，不影响插入操作，则正常返回
        return data
    # 如果是有意义的内容，需要处理，比如： 可口可乐\    内容中自带斜杠导致程序出错
    # 乱七八糟的符号，我们要处理掉
    data = data.replace("'", "")
    data = data.replace('"', "")
    data = data.replace("\\", "")
    data = data.replace(";", "")
    data = data.replace(",", "")
    data = data.replace("@", "")

    return data
