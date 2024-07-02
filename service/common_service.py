# -*- coding: UTF-8 -*-

from core.database import *
import calendar
from datetime import timedelta
import holidays
from datetime import datetime
import pandas as pd


def init_data(acc_entity, acc_period):
    logger.info("初始化或重新初始化数据")
    exec_command("""
update sys_config 
   set value = '"""+acc_entity+"""',
       update_time = strftime('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP, 'localtime')
where  key = 'sys.acc_entity'
""")
    exec_command("""
update sys_config 
   set value = '""" + acc_period + """',
       update_time = strftime('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP, 'localtime')
where  key = 'sys.acc_period'
    """)
    reset_workday(acc_period)
    reset_database()


def reset_workday(yearmonth):
    logger.info("重置工作日")

    dates = get_dates_in_month(yearmonth)
    cn_holidays = holidays.country_holidays('CN')
    workdays = []
    for date in dates:
        if cn_holidays.is_workday(date):
            workdays.append(date.day)
    df = pd.DataFrame(workdays, columns=['day'])
    df.to_sql("sys_workday", conn, if_exists="replace", index=False)


def get_dates_in_month(yearmonth):
    ym = datetime.strptime(yearmonth, '%Y%m')
    year = ym.year
    month = ym.month
    first_day_of_month = datetime(year, month, 1)
    days_in_month = calendar.monthrange(year, month)[1]

    dates = [first_day_of_month + timedelta(days=i) for i in range(days_in_month)]
    return dates


def reset_database():
    logger.info("重置数据库")
    cur = exec_query("""
select case when name like 'para_%' or name like 'orig_%' then 'delete from '||name
            else 'delete from '||name 
       end action
from sqlite_master
where type='table'
  and name not like 'sys_%' and name not like 'tpl_%' and name not like 'code_%'
order by 1    
    """)
    for row in cur:
        print(str(row[0]))
        exec_command(str(row[0]))

    exec_command("vacuum")

