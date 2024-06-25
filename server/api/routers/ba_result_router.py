# -*- coding: UTF-8 -*-
from typing import Union
from pydantic import BaseModel

from fastapi import UploadFile, File
from starlette.responses import Response

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


@ba_result_router.get("/tpl_para_voucher_summary_transfer", tags=["3.4 研发核算结果管理"])
async def tpl_para_voucher_summary_transfer():
    """
    获取ach_后台表中摘要更新规则表模板
    """
    try:
        file_for_download = "para_voucher_summary_transfer.xlsx"
        excel_content = db.export_through_mem("tpl_para_voucher_summary_transfer")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_result_router.post("/imp_para_voucher_summary_transfer", tags=["3.4 研发核算结果管理"])
async def imp_para_voucher_summary_transfer(file: UploadFile = File(...)):
    """
    导入ach_后台表中摘要更新规则表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "para_voucher_summary_transfer", False)
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


# @ba_result_router.get("/get_para_voucher_summary_transfer", tags=["3.4 研发核算结果管理"])
# async def get_para_voucher_summary_transfer():
#     """
#     ach_后台表中摘要更新规则
#     """
#     try:
#         data = db.get_table('para_voucher_summary_transfer', 1, 500)
#         return ok(data)
#     except sqlite3.OperationalError as oe:
#         logger.error("数据库操作异常：" + str(oe))
#         return fail(21, "数据库操作异常：" + str(oe))
#     except Exception as ex:
#         logger.error(ex)
#         return fail(24, str(ex))


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


@ba_result_router.get("/get_voucher_attach_vno", tags=["3.4 研发核算结果管理"])
async def get_voucher_attach_vno():
    """
    <b>凭证管理 1 需要整理附件的凭证</b> <br>
    """
    try:
        data = ba_result_service.get_voucher_attach_vno()
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


class VoucherNo(BaseModel):
    voucher_no: Union[str, None] = None


@ba_result_router.post("/get_voucher_attach_tabtitle", tags=["3.4 研发核算结果管理"])
async def get_voucher_attach_tabtitle(voucher_no: VoucherNo):
    """
    <b>凭证管理 2 获取标签页名称</b> <br>
    参数 voucher_no凭证号码 例如  ACH1000007
    """
    try:
        data = ba_result_service.get_voucher_attach_tabtitle(voucher_no.voucher_no)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


class TabInfo(BaseModel):
    voucher_no: Union[str, None] = None
    raccount_code: Union[str, None] = None
    sign: Union[str, None] = None


@ba_result_router.post("/get_voucher_attach_tabcontent", tags=["3.4 研发核算结果管理"])
async def get_voucher_attach_tabcontent(tab_info: TabInfo):
    """
    <b>凭证管理 3 获取标签页内容</b> <br>
    参数: <br>
       voucher_no 凭证号码, 例如  ACH1000007 <br>
       raccount_code 研发科目代码, 例如 5301010020020 <br>
       sign 符号(取值: {p:正数,n:负数})，例如 p
    """
    try:
        data = ba_result_service.get_voucher_attach_tabcontent(tab_info.voucher_no, tab_info.raccount_code, tab_info.sign)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))