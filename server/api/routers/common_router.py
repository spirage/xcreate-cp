# -*- coding: UTF-8 -*-
import service.common_service as common_service
from core.response import *
from fastapi.routing import APIRouter
from typing import Union
from pydantic import BaseModel
import core.database as db

common_router = APIRouter()


class Param(BaseModel):
     acc_entity: Union[str, None] = None
     acc_period: Union[str, None] = None


@common_router.post("/init_data", tags=["2 通用接口"])
async def init_data(param: Param):
    """
    <b>初始化或重新初始化数据</b> <br>
    功能：<br>
       设置CP核心处理单元的当前会计主体和会计期并同时重置工作日和数据库 <br>
    参数：<br>
       acc_entity: 会计主体 一般为公司名称 例如 ***集团公司 <br>
       acc_period: 会计期 格式为年月字符串 例如 202312
    """
    common_service.init_data(param.acc_entity, param.acc_period)
    return ok()

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
