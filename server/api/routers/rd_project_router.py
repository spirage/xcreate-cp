# -*- coding: UTF-8 -*-
from typing import Union
from pydantic import BaseModel

from fastapi import UploadFile, File
from starlette.responses import Response

from service.rd_project_service import *
from core.response import *
from fastapi.routing import APIRouter
import service.rd_project_service as rd_service
import core.database as db

rd_project_router = APIRouter()


@rd_project_router.get("/tpl_para_project", tags=["3.2 研发项目分析"])
async def tpl_para_project():
    """
    获取研发项目人工设置参数表模板
    """
    try:
        file_for_download = "para_project.xlsx"
        excel_content = db.export_through_mem("tpl_para_project")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.post("/imp_para_project", tags=["3.2 研发项目分析"])
async def imp_para_project(file: UploadFile = File(...)):
    """
    导入研发项目人工设置参数表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "para_project")
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.get("/tag_mat_group", tags=["3.2 研发项目分析"])
async def tag_mat_group():
    """
    项目标记、多项标记、首次投入标记、首次产出标记等数据标签处理。
    """
    try:
        rd_service.tag_mat_group()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))
    return ok()


@rd_project_router.get("/get_stat_project_wt", tags=["3.2 研发项目分析"])
async def get_stat_project_wt():
    """
    处理和获取研发项目匹配结果统计表
    """
    try:
        rd_service.process_stat_project_wt()
        data = db.get_table('stat_project_wt', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.get("/exp_code_cost_account", tags=["3.2 研发项目分析"])
async def exp_code_cost_account():
    """
    导出当前成本科目代码表
    """
    try:
        file_for_download = "code_cost_account.xlsx"
        excel_content = db.export_through_mem("code_cost_account")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.post("/imp_code_cost_account", tags=["3.2 研发项目分析"])
async def imp_code_cost_account(file: UploadFile = File(...)):
    """
    导入成本科目代码表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "code_cost_account", False)
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.get("/exp_code_cost_center", tags=["3.2 研发项目分析"])
async def exp_code_cost_center():
    """
    导出当前成本中心代码表
    """
    try:
        file_for_download = "code_cost_center.xlsx"
        excel_content = db.export_through_mem("code_cost_center")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.post("/imp_code_cost_center", tags=["3.2 研发项目分析"])
async def imp_code_cost_center(file: UploadFile = File(...)):
    """
    导入导入成本中心代码表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "code_cost_center", False)
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.get("/tpl_product_cost", tags=["3.2 研发项目分析"])
async def tpl_product_cost():
    """
    获取成本中心产品成本构成表模板
    """
    try:
        file_for_download = "orig_product_cost.xlsx"
        excel_content = db.export_through_mem("tpl_product_cost")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.post("/imp_product_cost", tags=["3.2 研发项目分析"])
async def imp_product_cost(file: UploadFile = File(...)):
    """
    导入成本中心产品成本构成表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "orig_product_cost")
            rd_service.fill_code_product()
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


class Page(BaseModel):
    page: Union[int, None] = 1
    limit: Union[int, None] = 500


@rd_project_router.post("/get_semi_product_in_out", tags=["3.2 研发项目分析"])
async def get_semi_product_in_out(page: Page):
    """
    aca_bb_半成品投入产出
    """
    try:
        rd_service.process_semi_product_in_out()
        data = db.get_table('semi_product_in_out', page.page, page.limit)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.post("/get_semi_product_in_out_sum", tags=["3.2 研发项目分析"])
async def get_semi_product_in_out_sum(page: Page):
    """
    aca_bc_半成品投入产出归集
    """
    try:
        rd_service.process_semi_product_in_out_sum()
        data = db.get_table('semi_product_in_out_sum', page.page, page.limit)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.get("/get_map_project_product", tags=["3.2 研发项目分析"])
async def get_map_project_product():
    """
    aca_ca_首次分配数据源 处理和获取项目产品对应关系表
    """
    try:
        rd_service.process_map_project_product()
        data = db.get_table('map_project_product', 1, 1000)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


# 新增 0618
@rd_project_router.get("/exp_instorage_tag", tags=["3.2 研发项目分析"])
async def exp_instorage_tag():
    """
    获取当前入库标记信息
    """
    try:
        rd_service.get_instorage()
        file_for_download = "para_instorage_tag.xlsx"
        excel_content = db.export_through_mem("para_instorage_tag")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


# 新增 0618
@rd_project_router.post("/imp_instorage_tag", tags=["3.2 研发项目分析"])
async def imp_instorage_tag(file: UploadFile = File(...)):
    """
    导入并更新入库标记信息
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "para_instorage_tag", False)
            rd_service.tag_instorage()
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.post("/get_aca_db", tags=["3.2 研发项目分析"])
async def get_aca_db(page: Page):
    """
    aca_db_中间试验品数据源
    """
    try:
        rd_service.process_aca_db()
        data = db.get_table('aca_db', page.page, page.limit)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.post("/get_aca_dc", tags=["3.2 研发项目分析"])
async def get_aca_dc(page: Page):
    """
    aca_dc_转库存履历码产出明细
    """
    try:
        rd_service.process_aca_dc()
        data = db.get_table('aca_dc', page.page, page.limit)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@rd_project_router.get("/get_aca_dd", tags=["3.2 研发项目分析"])
async def get_aca_dd():
    """
    aca_dd_转库存履历码产出归集
    """
    try:
        rd_service.process_aca_dd()
        data = db.get_table('aca_dd', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))
