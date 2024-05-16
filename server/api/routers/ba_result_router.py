# -*- coding: UTF-8 -*-
from typing import Union
from pydantic import BaseModel
from service.ba_mat_prod_service import *
from core.response import *
from fastapi.routing import APIRouter
import service.ba_result_service as ba_result_service
import core.database as db

ba_result_router = APIRouter()


class Page(BaseModel):
    page: Union[int, None] = 1
    limit: Union[int, None] = 500


@ba_result_router.get("/get_voucher_recalculated", tags=["3.4 研发核算结果管理"])
async def get_voucher_recalculated():
    """
    ach_acg_单价重算后凭证
    """
    try:
        ba_result_service.process_voucher_recalculated()
        data = db.get_table('vucher_recalculated', 1, 2000)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_result_router.get("/get_para_voucher_summary_transfer", tags=["3.4 研发核算结果管理"])
async def get_para_voucher_summary_transfer():
    """
    ach_后台表中摘要更新规则
    """
    try:
        data = db.get_table('para_voucher_summary_transfer', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_result_router.post("/get_voucher_merged", tags=["3.4 研发核算结果管理"])
async def get_voucher_merged(page: Page):
    """
    ach_fa_记账凭证数据合并
    """
    try:
        ba_result_service.process_voucher_merged()
        data = db.get_table('voucher_merged', page.page, page.limit)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_result_router.get("/get_stat_voucher", tags=["3.4 研发核算结果管理"])
async def get_stat_voucher():
    """
    ach_fb_记账凭证信息统计
    """
    try:
        ba_result_service.process_stat_voucher()
        data = db.get_table('stat_voucher', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_result_router.post("/get_voucher_output", tags=["3.4 研发核算结果管理"])
async def get_voucher_output(page: Page):
    """
    ach_导入页
    """
    try:
        ba_result_service.process_voucher_output()
        data = db.get_table('voucher_output', page.page, page.limit)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))