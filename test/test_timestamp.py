from unittest import TestCase
from util import time_util as ts
import time


class TestTimeStampUtil(TestCase):
    def setUp(self) -> None:
        pass

    def test_get_second_tm(self):
        result = ts.GetTimeStamp.get_second_tm()
        times = int(time.time())
        self.assertEqual(times, result)

    def test_get_millisecond_tm(self):
        result = ts.GetTimeStamp.get_millisecond_tm()
        t = time.time()
        times = int(round(t * 1000))
        self.assertEqual(times, result)

    def test_get_date(self):
        times = ts.GetTimeStamp.get_second_tm()
        test_date = ts.GetTimeStamp.get_date(times)
        date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        self.assertEqual(date, test_date)

        time_s = ts.GetTimeStamp.get_millisecond_tm()
        test_date_s = ts.GetTimeStamp.get_date(time_s)
        date_s = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        self.assertEqual(test_date_s, date_s)
