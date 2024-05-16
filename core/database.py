# -*- coding: UTF-8 -*-
import io
import math

# from core.config import *
from core.log import *
import sqlite3
import os
import pandas as pd

db_file = 'xcp.db'
conn = sqlite3.connect(db_file)


def total_count(table_name, where_clause=None):
    logger.info("开始统计数据量 表名:[" + table_name + "], 条件:[" + where_clause + "]")
    query = "select count(*) from " + table_name
    if where_clause is not None and where_clause != "":
        query += " where " + where_clause
    cur = exec_query(query)
    return cur.fetchone()[0]


def exec_script(script_file):
    with open(script_file, 'r', encoding='utf-8') as script:
        sql_script = script.read()
        with sqlite3.connect(db_file) as conn:
            logger.info("开始执行脚本 [" + script_file + "]")
            conn.executescript(sql_script)


def exec_query(query):
    logger.debug("开始执行查询 [" + query + "]")
    cur = conn.cursor()
    cur.execute(query)
    return cur


def exec_command(command):
    logger.debug("开始执行命令 [" + command + "]")
    result = conn.execute(command)
    conn.commit()
    return result


def scan_excel(folder, prefix):
    excels_found = []

    for top, sub, files in os.walk(folder):
        for file in files:
            file_type = file.split(".")[1]
            if prefix and file.startswith(prefix) and file_type in ["xlsx", "xls"]:
                excels_found.append((top, file))
    return excels_found


def load_excel(file, table_name):
    try:
        dict_excel = pd.read_excel(file, sheet_name=None, dtype=str)
        i = 0
        for key in dict_excel.keys():
            sheet = dict_excel[key]
            if i == 0:
                sheet.to_sql(table_name, conn, if_exists="replace")
                i += 1
            else:
                sheet.to_sql(table_name, conn, if_exists="append")
                i += 1
    except FileNotFoundError:
        logger.error("Excel文件 [" + file + "] 不存在")
    except sqlite3.OperationalError as oe:
        logger.error(oe)
        logger.error("Excel文件Sheet文件格式不一致，请仔细检查不同Sheet中标题行(第一行)名称")


# def import_code_tables(excel_name=""):
#     folder_code_tables = work_home + "\\代码表"
#     files = scan_excel(folder_code_tables, "code_" + excel_name)
#     for file in files:
#         logger.info("导入代码表 [" + file + "]")
#         load_excel(folder_code_tables + "\\" + file, file.split(".")[0])
#
#
# def import_para_tables(excel_name=""):
#     folder_para_tables = work_home + "\\参数表"
#     files = scan_excel(folder_para_tables, "para_" + excel_name)
#     for file in files:
#         logger.info("导入参数表 [" + file + "]")
#         load_excel(folder_para_tables + "\\" + file, file.split(".")[0])
#
#
# def import_orig_tables(excel_name=""):
#     folder_orig_tables = work_home + "\\原始表"
#     files = scan_excel(folder_orig_tables, "orig_" + excel_name)
#     for file in files:
#         logger.info("导入原始表 [" + file + "]")
#         load_excel(folder_orig_tables + "\\" + file, file.split(".")[0])
#
#
# def import_table(excel_name=""):
#     files = scan_excel(work_home, excel_name)
#     if files is None or len(files) == 0:
#         raise FileNotFoundError
#     for file in files:
#         logger.info("导入数据表 [" + file[1] + "]")
#         load_excel(os.path.join(file[0], file[1]), file[1].split(".")[0])


async def import_through_mem(file_uploaded, table_name, import_index=True):
    logger.info("通过内存导入表 [" + table_name + "]")
    bytes_io = io.BytesIO()
    bytes_io.write(await file_uploaded.read())
    bytes_io.seek(0)
    df = pd.read_excel(bytes_io, dtype=str)
    exec_command("delete from " + table_name)
    #if import_index:
        #df = df.reset_index(drop=False).rename(columns={'index': 'rowno'})
    df.to_sql(table_name, conn, if_exists="append", index=import_index)


# def export_table(table_name):
#     logger.info("导出表 [" + table_name + "]")
#     folder_export = work_home + "\\导出表\\"
#     file = folder_export + table_name + ".xlsx"
#     query = "select * from " + table_name
#     df = pd.read_sql_query(query, conn)
#     df.to_excel(file, index=False, engine='openpyxl')


def export_through_mem(table_name):
    logger.info("通过内存导出表 [" + table_name + "]")
    query = "select * from " + table_name
    df = pd.read_sql_query(query, conn)
    excel_bytes_io = io.BytesIO()
    with pd.ExcelWriter(excel_bytes_io, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    excel_bytes_io.seek(0)
    return excel_bytes_io.read()


def page_helper(query, page=1, limit=500):
    sql_count = 'select count(*) from ('+query+')'
    cur_count = exec_query(sql_count)
    total = int(cur_count.fetchone()[0])
    pages = math.ceil(total / limit)
    offset = str((page - 1) * limit)
    sql_list = query + ' limit ' + str(limit) + ' offset ' + offset
    cur_list = exec_query(sql_list)
    rows = cur_list.fetchall()
    rst_list = [dict(zip(tuple(column[0] for column in cur_list.description), row)) for row in rows]
    return {'total': total, 'pages': pages, 'limit': limit, 'page': page, 'list': rst_list}


def get_table(table_name, page=1, limit=500):
    return page_helper('select * from ' + table_name, page, limit)
