# -*- coding: UTF-8 -*-
from datetime import datetime
import chinese_calendar as calendar
import pandas as pd


def get_workdays(yearmonth):
    ym = datetime.strptime(yearmonth, '%Y%m')
    year = ym.year
    month = ym.month
    workdays = []
    for d in calendar.get_workdays(datetime(year, month, 1), datetime(year, month, 31)):
        workdays.append(d.day)
    df = pd.DataFrame(workdays, columns=['day'])
    df.to_excel('workdays-' + yearmonth + '.xlsx', index=False, engine='openpyxl')


if __name__ == '__main__':
    get_workdays('202312')

