import time


class GetTimeStamp(object):
    @staticmethod
    def get_second_tm():
        """
        获取秒级时间戳
        :return: 返回秒级时间戳
        """
        ts = int(time.time())
        return ts

    @staticmethod
    def get_millisecond_tm():
        """
        获取毫秒级时间戳
        :return:
        """
        ts_mm = (int(round(time.time() * 1000)))
        return ts_mm

    @staticmethod
    def get_date(times, format_time="%Y-%m-%d %H:%M:%S"):
        """
        根据时间戳返回时间格式
        :param times: 时间戳
        :param format_time: 返回格式，有默认值
        :return: 返回时间格式
        """
        if len(str(times)) == 13:
            time_array = time.localtime(int(times / 1000))
            time_style = time.strftime(format_time, time_array)
        else:
            time_array = time.localtime(times)
            time_style = time.strftime(format_time, time_array)
        return time_style
