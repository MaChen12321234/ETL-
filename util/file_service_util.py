'''
这个工具用来判断那些文件我们能用。
'''
import os
from util import logging_util

logger = logging_util.init_log()


def file_service_util_list(path='./', recursion=False):
    '''
    在传参的文件夹中搜索需要的数据文件
    :param path: 文件路径
    :param recursion: 是否递归
    :return: 文件列表
    '''
    # 获取path下的文件列表
    file_part_list = os.listdir(path)
    # print(file_part_list)
    # 定义空列表，存放文件
    files = []
    # 循环判断获取的文件列表是文件还是文件夹
    for file_name in file_part_list:
        file_full = f'{path}/{file_name}'
        # 判断，如果是文件，则直接放入files
        if not os.path.isdir(file_full):
            # 拼接
            files.append(file_full)
        else:
            # 如果是文件夹，判断是否递归
            if recursion:
                file_recursion = file_service_util_list(file_full, recursion=recursion)
                # 拼接内层递归得到的list到files中
                files += file_recursion
    logger.info(f"获取{path}中所有文件如下：{files}")
    return files


def get_need_file_data(all_list, processed_list):
    """
    该方法通过对比两组列表，得到一个新的需要执行的数据文件列表
    :param all_list: 全部列表
    :param processed_list: 已经执行过的列表数据
    :return: 返回一个新的列表用于存储没有执行过的文件
    """
    # 定义一个新的列表，存储需要执行的文件
    need_file = []
    # 通过for循环来比对
    for alone_data in all_list:
        if alone_data not in processed_list:
            need_file.append(alone_data)
    logger.info(f"获取需要执行的文件：{need_file}")
    return need_file
