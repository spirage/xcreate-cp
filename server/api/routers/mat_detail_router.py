# -*- coding: UTF-8 -*-
from typing import Union
from pydantic import BaseModel

from fastapi import UploadFile, File
from starlette.responses import Response

from service.mat_detail_service import *
from core.response import *
from fastapi.routing import APIRouter
import service.mat_detail_service as mat_service
import core.database as db

mat_detail_router = APIRouter()


@mat_detail_router.get("/tpl_mat_track", tags=["3.1 物料详情处理"])
async def tpl_mat_track():
    """
    获取物料追踪表（履历表）模板
    """
    try:
        file_for_download = "orig_mat_track.xlsx"
        excel_content = db.export_through_mem("tpl_mat_track")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@mat_detail_router.post("/imp_mat_track", tags=["3.1 物料详情处理"])
async def imp_mat_track(file: UploadFile = File(...)):
    """
    导入物料追踪表（履历表）数据
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "orig_mat_track")
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@mat_detail_router.get("/tpl_trans_record", tags=["3.1 物料详情处理"])
async def tpl_trans_record():
    """
    获取交易记录表（日档表）模板
    """
    try:
        file_for_download = "orig_trans_record.xlsx"
        excel_content = db.export_through_mem("tpl_trans_record")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@mat_detail_router.post("/imp_trans_record", tags=["3.1 物料详情处理"])
async def imp_trans_record(file: UploadFile = File(...)):
    """
    导入物料追踪表（履历表）数据
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "orig_trans_record")
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@mat_detail_router.get("/gen_mat_detail", tags=["3.1 物料详情处理"])
async def gen_mat_detail():
    """
    生成物料详情信息
    """
    try:
        mat_service.gen_mat_detail()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))
    return ok()


class Page(BaseModel):
    page: Union[int, None] = 1
    limit: Union[int, None] = 500


@mat_detail_router.post("/get_mat_detail", tags=["3.1 物料详情处理"])
async def get_mat_detail(page: Page):
    """
    获取物料详情信息
    """
    try:
        data = db.get_table("mat_track_detail", page.page, page.limit)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@mat_detail_router.get("/exp_mat_detail", tags=["3.1 物料详情处理"])
async def exp_mat_detail():
    """
    导出当前物料详情信息
    """
    try:
        file_for_download = "mat_track_detail.xlsx"
        excel_content = db.export_through_mem("mat_track_detail", exclude="row_no")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@mat_detail_router.post("/imp_mat_detail", tags=["3.1 物料详情处理"])
async def imp_mat_detail(file: UploadFile = File(...)):
    """
    导入物料详情信息
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "mat_track_detail", import_index=False, rename_index=True)
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))
