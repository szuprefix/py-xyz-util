# -*- coding:utf-8 -*-
import calendar
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from six import string_types
__author__ = 'denishuang'


def format_the_date(the_date=None):
    if not the_date:
        return date.today()
    if isinstance(the_date, string_types):
        return datetime.strptime(the_date[:10], "%Y-%m-%d").date()
    if isinstance(the_date, datetime):
        return the_date.date()
    return the_date


def get_date_by_day(day, today=None):
    today = format_the_date(today)
    endday = calendar.monthrange(today.year, today.month)[1]
    if day > endday:
        day = endday
    return date(today.year, today.month, day)


def get_next_date(the_date=None, days=1):
    the_date = format_the_date(the_date)
    return the_date + timedelta(days=days)

def date_range(begin_date, end_date):
    d = format_the_date(begin_date)
    end_date = format_the_date(end_date)
    if end_date<d:
        raise ValueError('end_date should be bigger than begin_date')
    while end_date >= d:
        yield d
        d = d+timedelta(days=1)

def get_this_and_next_monday(the_date=None):
    the_date = format_the_date(the_date)
    monday = the_date + timedelta(days=-the_date.weekday())
    next_monday = monday + timedelta(days=7)
    return monday, next_monday


def get_monday_and_sunday(the_date=None):
    the_date = format_the_date(the_date)
    monday = the_date + timedelta(days=-the_date.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def get_this_and_next_month_first_day(the_date=None):
    the_date = format_the_date(the_date)
    month_first_day = date(the_date.year, the_date.month, 1)
    next_month_first_day = month_first_day + relativedelta(months=1)
    return month_first_day, next_month_first_day


def get_last_and_this_month_first_day(the_date=None):
    the_date = format_the_date(the_date)
    this_month_first_day = date(the_date.year, the_date.month, 1)
    last_month_first_day = this_month_first_day + relativedelta(months=-1)
    return last_month_first_day, this_month_first_day


def get_month_first_day_from_now(the_date=None, month_delta=0):
    the_date = format_the_date(the_date)
    month_first_day = date(the_date.year, the_date.month, 1)
    return month_first_day + relativedelta(months=month_delta)


def get_current_year_last_month():
    this_day = date.today()
    last_month_this_day = this_day + relativedelta(months=-1)
    return last_month_this_day.strftime('%Y-%m')


def get_current_month(the_date=None):
    the_date = format_the_date(the_date)
    return the_date.strftime('%Y-%m')


def weeks_between(d1, d2):
    if isinstance(d1, string_types):
        d = d1.split('-')
        d1 = datetime(int(d[0]), int(d[1]), int(d[2]))
    if isinstance(d2, string_types):
        d = d2.split('-')
        d2 = datetime(int(d[0]), int(d[1]), int(d[2]))
    return abs(d2.isocalendar()[1] - d1.isocalendar()[1])


def get_week_of_month(d=None):
    if d is None:
        d = datetime.now()
    week_of_year_end = d.strftime("%W")
    week_of_year_begin = datetime(d.year, d.month, 1).strftime("%W")
    return int(week_of_year_end) - int(week_of_year_begin) + 1


def get_pre_month(d=None):
    import calendar
    if d is None:
        d = datetime.now()
    if isinstance(d, string_types):
        d = datetime(int(d.split('-')[0]), int(d.split('-')[1]), 1)
    this_month_days = calendar.monthrange(d.year, d.month)[1]
    the_day_of_pre_month = d - timedelta(days=this_month_days)
    return datetime.strftime(the_day_of_pre_month, '%Y-%m')


def first_date_of_month(month=None):
    if month:
        return datetime.strptime("%s-01" % month, "%Y-%m-%d")
    d = datetime.now()
    return date(d.year, d.month, 1)


def get_age(born):
    today = date.today()
    try:
        birthday = born.replace(year=today.year)
    except ValueError:
        # raised when birth date is February 29
        # and the current year is not a leap year
        birthday = born.replace(year=today.year, day=born.day - 1)
    if birthday > today:
        return today.year - born.year - 1
    else:
        return today.year - born.year


def is_valid_date(strdate):
    """判断是否是一个有效的日期字符串"""
    try:
        if ":" in strdate:
            datetime.strptime(strdate, "%Y-%m-%d %H:%M:%S")
        else:
            datetime.strptime(strdate, "%Y-%m-%d")
        return True
    except:
        return False

def get_period_by_name(name):
    from .dateutils import get_next_date, format_the_date
    from datetime import datetime, timedelta
    if name == "今天":
        begin_time = format_the_date()
        end_time = get_next_date(begin_time)
    elif name == "昨天":
        end_time = format_the_date()
        begin_time = get_next_date(end_time, -1)
    elif name.startswith("近") and name.endswith("分钟"):
        end_time = datetime.now()
        begin_time = end_time + timedelta(minutes=-int(name[1:-2]))
    elif name.startswith("近") and name.endswith("小时"):
        end_time = datetime.now()
        begin_time = end_time + timedelta(hours=-int(name[1:-2]))
    elif name.startswith("近") and name.endswith("天"):
        end_time = get_next_date()
        begin_time = get_next_date(end_time, -int(name[1:-1]))
    elif "至" in name:
        drs = name.split("至")
        begin_time = format_the_date(drs[0])
        end_time = get_next_date(drs[1])
    else:
        raise ValueError("日期范围格式不正确:%s" % name)
    return (begin_time, end_time)

COMMON_DATES = {
    'today': lambda d=None: format_the_date(the_date=d).isoformat(),
    'this_month': lambda d=None: get_current_month(the_date=d),
    'this_year': lambda d=None: format_the_date(the_date=d).year,
    'yesterday': lambda d=None: get_next_date(the_date=d,days=-1).isoformat(),
    'tomorrow': lambda d=None: get_next_date(the_date=d,days=1).isoformat(),
    'week_begin': lambda d=None: get_monday_and_sunday(the_date=d)[0].isoformat(),
    'week_end': lambda d=None: get_monday_and_sunday(the_date=d)[1].isoformat(),
    'month_begin': lambda d=None: get_month_first_day_from_now(the_date=d).isoformat(),
    'month_end': lambda d=None: get_next_date(get_this_and_next_month_first_day(the_date=d)[1], -1).isoformat(),
    'next_month_begin': lambda d=None: get_this_and_next_month_first_day(the_date=d)[1].isoformat()
}
