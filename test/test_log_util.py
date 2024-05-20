'''
测试日志工具的功能单元测试
'''
import logging
from unittest import TestCase
from util import logging_util as log_util


class TestLogUtil(TestCase):
    def setUp(self) -> None:
        pass

    def test_init_log(self):
        log_test = log_util.init_log()
        self.assertIsInstance(log_test, logging.RootLogger)