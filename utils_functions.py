from tzlocal import get_localzone
import datetime
import re
from dateutil import parser as time_parser
import rfc3339
from dateutil.relativedelta import relativedelta

import dateutil.parser as date_parserZ


def get_numeric_offset(offset_str, offset_pattern):
    m = re.search(offset_pattern, offset_str)
    if m:
        return int(m.group(0)[:-1])
    return 0


def calculate_timedelta_offset(offset_str):
    # FIXME: resi sa ove dve funkcije, ali budi siguran da dobro rade u oba slucaja
    year_offset = get_numeric_offset(offset_str, "\d+(y|Y)")
    # print("Year: %d" % year_offset)
    month_offset = get_numeric_offset(offset_str, "\d+M")
    # print("Month: %d" % month_offset)
    day_offset = get_numeric_offset(offset_str, "\d+(d|D)")
    # print("Day: %d" % day_offset)

    hour_offset = get_numeric_offset(offset_str, "\d+(h|H)")
    # print("Hour: %d" % hour_offset)
    minute_offset = get_numeric_offset(offset_str, "\d+m")
    # print("Minute: %d" % minute_offset)
    second_offset = get_numeric_offset(offset_str, "\d+(s|S)")
    # print("Second: %d" % second_offset)

    offset_delta = relativedelta(years=year_offset, months=month_offset, days=day_offset,
                                 hours=hour_offset, minutes=minute_offset, seconds=second_offset)
    return offset_delta


# FIXME: fow now months and years are not supported
def convert_timedelta_offset_to_seconds(timedelta_offset):
    return timedelta_offset.seconds + timedelta_offset.minutes * 60 + timedelta_offset.hours * 3600 \
           + timedelta_offset.days * 24 * 3600


def get_current_local_time():
    local = get_localzone()
    return datetime.datetime.now(local)


def get_local_timezone():
    local = get_localzone()
    tz = get_current_local_time().strftime('%z')
    # FIXME: Mongo ne prihava dve tacke u casovnoj zoni
    # tz = "%s:%s" % (tz[:3], tz[3:])
    return tz


def removo_colon_from_rfc3339_time_format(rfc3339_str):
    # da li ima casovne zone
    if "+" not in rfc3339_str:
        return rfc3339
    # delimo na vreme i casovnu zonu
    t, tz = rfc3339_str.strip().split("+")
    # uklanjamo dve tacke iz casovne zone
    tz = re.sub(r":", "", tz)
    # spajamo vreme i casovnu zonu
    return "%s+%s" % (t, tz)


class DateTimeInterval(object):
    def __init__(self, datetime_str):
        self.start_time = time_parser.parse(datetime_str)
        self.end_time = None

    def __getitem__(self, item):
        if item == 0:
            # da izbacimo :
            return removo_colon_from_rfc3339_time_format(rfc3339.rfc3339(self.start_time))
        elif item == 1:
            return removo_colon_from_rfc3339_time_format(rfc3339.rfc3339(self.end_time))
        else:
            return None


class SecondInterval(DateTimeInterval):
    def __init__(self, datetime_str):
        super().__init__(datetime_str)
        self.end_time = self.start_time + relativedelta(seconds=1)


class MinuteInterval(DateTimeInterval):
    def __init__(self, datetime_str):
        super().__init__(datetime_str)
        self.end_time = self.start_time + relativedelta(minutes=1)


class HourInterval(DateTimeInterval):
    def __init__(self, datetime_str):
        super().__init__(datetime_str)
        self.end_time = self.start_time + relativedelta(hours=1)


class DayInterval(DateTimeInterval):
    def __init__(self, datetime_str):
        super().__init__(datetime_str)
        self.end_time = self.start_time + relativedelta(days=1)


class MonthInterval(DateTimeInterval):
    def __init__(self, datetime_str):
        super().__init__(datetime_str)
        self.end_time = self.start_time + relativedelta(months=1)


class YearInterval(DateTimeInterval):
    def __init__(self, datetime_str):
        super().__init__(datetime_str)
        self.end_time = self.start_time + relativedelta(years=1)


if __name__ == '__main__':
    y = YearInterval("2014")
    print(y.start_time)
    print(y.end_time)
