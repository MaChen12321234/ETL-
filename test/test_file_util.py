import os
from unittest import TestCase
from util import file_service_util as file
'''
单元测试：查找文件的工具方法进行单元测试
'''

class TestFileUtil(TestCase):
    def setUp(self) -> None:
        self.get_down_path = os.path.dirname(os.getcwd())

    def get_file_name(self, file_name):
        names = []
        for name in file_name:
            names.append(os.path.basename(name))
        names.sort()
        return names

    def test_file_service_util_list(self):
        """
        请在工程根目录的test文件夹内建立:test_dir/inner1/
                    inner2/
        的目录结构用于进行此方法的单元测试
        不递归结果应该是1和2
        递归结果应该是1，2，3，4，5
        :return:
        """
        result_no_recursion = file.file_service_util_list(path=f'{self.get_down_path}/test/test_dir', recursion=False)
        end_no_recursion = self.get_file_name(result_no_recursion)

        self.assertEqual(['1.txt', '2.txt'], end_no_recursion)
        result_recursion = file.file_service_util_list(path=f'{self.get_down_path}/test/test_dir', recursion=True)
        end_recursion = self.get_file_name(result_recursion)
        self.assertEqual(['1.txt', '2.txt', '3.txt', '4.txt', '5.txt'], end_recursion)

    def test_get_need_file_data(self):
        """
        创建两个list数据进行比对
        :return:
        """
        list_all = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        list_processed = [2, 3, 4, 5]
        result = file.get_need_file_data(list_all, list_processed)
        self.assertEqual([1, 6, 7, 8, 9, 10], result)
