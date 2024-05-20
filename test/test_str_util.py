from unittest import TestCase
from util import str_util


class TestStrUtil(TestCase):
    def setUp(self) -> None:
        pass

    def test_check_str_undefined(self):
        a = "None"
        result1 = str_util.check_str_undefined(a)
        print(result1)
        self.assertTrue(result1)

        b = "null"
        result2 = str_util.check_str_undefined(b)
        self.assertTrue(result2)

        c = "undefined"
        result3 = str_util.check_str_undefined(c)
        self.assertTrue(result3)

        d = "sad"
        result4 = str_util.check_str_undefined(d)
        self.assertFalse(result4)

        e = None
        result5 = str_util.check_str_undefined(e)
        self.assertTrue(result5)

    def test_check_str_null_and_transform_to_sql_null(self):
        a = ""
        result = str_util.check_str_null_and_transform_to_sql_null(a)
        sql = f"INSERT INTO table VALUES(1, ASD, {result})"
        self.assertEqual("INSERT INTO table VALUES(1, ASD, NULL)", sql)
