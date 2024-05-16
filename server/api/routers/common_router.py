# -*- coding: UTF-8 -*-
from service.mat_detail_service import *
from core.response import *
from fastapi.routing import APIRouter
from typing import Union
from pydantic import BaseModel
import core.database as db

common_router = APIRouter()

#
# @common_router.get("/init_data", tags=["2 通用接口"])
# async def init_data():
#     """
#     初始化数据
#     """
#     db.import_code_tables()
#     db.import_para_tables()
#     db.import_orig_tables()
#     return ok()
#
#
# class Table(BaseModel):
#     table_name: str


# @common_router.post("/import_table", tags=["2 通用接口"])
# async def import_table(table: Table):
#     """
#     导入表
#     """
#     try:
#         db.import_table(table.table_name)
#         return ok()
#     except json.decoder.JSONDecodeError:
#         logger.error("POST请求中JSON格式不规范")
#         return fail(11, "POST请求中JSON格式不规范，无法解析")
#     except KeyError:
#         logger.error("POST请求中JSON内容不规范")
#         return fail(12, "POST请求中JSON内容不规范，请检查键名称")
#     except FileNotFoundError:
#         logger.error("文件不存在")
#         return fail(13, "文件不存在")
#     except Exception as ex:
#         logger.error(ex)
#         return fail(14, str(ex))


# @common_router.post("/export_table", tags=["2 通用接口"])
# async def export_table(table: Table):
#     """
#     导出表
#     """
#     try:
#         db.export_table(table.table_name)
#         return ok()
#     except json.decoder.JSONDecodeError:
#         logger.error("POST请求中JSON格式不规范")
#         return fail(11, "POST请求中JSON格式不规范，无法解析")
#     except KeyError:
#         logger.error("POST请求中JSON内容不规范")
#         return fail(12, "POST请求中JSON内容不规范，请检查键名称")
#     except Exception as ex:
#         logger.error(ex)
#         return fail(14, str(ex))


# class Page(BaseModel):
#     table_name: str
#     page: Union[int, None] = 1
#     limit: Union[int, None] = 500
#
#
# @common_router.post("/get_table", tags=["2 通用接口"])
# async def get_table(page: Page):
#     """
#     查询表
#     """
#     try:
#         data = db.get_table(page.table_name, page.page, page.limit)
#         return ok(data)
#     except sqlite3.OperationalError as oe:
#         logger.error("数据库操作异常：" + str(oe))
#         return fail(11, "数据库操作异常：" + str(oe))
#     except Exception as ex:
#         logger.error(ex)
#         return fail(14, str(ex))
