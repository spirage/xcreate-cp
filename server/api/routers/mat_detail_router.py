# -*- coding: UTF-8 -*-
import io
from typing import Union
from fastapi import UploadFile, File
from pydantic import BaseModel
from starlette.responses import FileResponse, Response, JSONResponse

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
    生成物料详情
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


@mat_detail_router.get("/set_mat_group", tags=["3.1 物料详情处理"])
async def set_mat_group():
    """
    设置物料详情分组
    """
    try:
        mat_service.set_mat_group()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))
    return ok()


@mat_detail_router.get("/fill_mat_detail", tags=["3.1 物料详情处理"])
async def fill_mat_detail():
    """
    补充物料详情信息
    """
    try:
        mat_service.fill_mat_detail()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))
    return ok()


