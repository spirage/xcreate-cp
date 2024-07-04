# -*- coding: UTF-8 -*-
from typing import Union
from fastapi import UploadFile, File
from pydantic import BaseModel
from starlette.responses import Response

from service.ba_mat_prod_service import *
from core.response import *
from fastapi.routing import APIRouter
import service.ba_mat_prod_service as ba_mat_prod_service
import core.database as db

ba_mat_prod_router = APIRouter()


class Page(BaseModel):
    page: Union[int, None] = 1
    limit: Union[int, None] = 500


@ba_mat_prod_router.get("/get_acg_ba_semi_voucher", tags=["3.3.2.2 生产物料核算"])
async def get_acg_ba_semi_voucher():
    """
    acg_ba_待调整序时账
    """
    try:
        ba_mat_prod_service.process_acg_ba_semi_voucher()
        data = db.get_table('acg_ba_待调整序时账', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_da_rd_unit_price", tags=["3.3.2.2 生产物料核算"])
async def get_acg_da_rd_unit_price():
    """
    acg_da_研发单价重算
    """
    try:
        ba_mat_prod_service.process_acg_da_rd_unit_price()
        data = db.get_table('acg_da_研发单价重算', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_db_rd_amount", tags=["3.3.2.2 生产物料核算"])
async def get_acg_db_rd_amount():
    """
    acg_db_研发金额重算
    """
    try:
        ba_mat_prod_service.process_acg_db_rd_amount()
        data = db.get_table('acg_db_研发金额重算', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/tpl_inventory_summary", tags=["3.3.2.2 生产物料核算"])
async def tpl_inventory_summary():
    """
    获取 acg_bb_上期收发存结果表 模板
    """
    try:
        file_for_download = "para_inventory_summary.xlsx"
        excel_content = db.export_through_mem("tpl_para_inventory_summary")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.post("/imp_inventory_summary", tags=["3.3.2.2 生产物料核算"])
async def imp_inventory_summary(file: UploadFile = File(...)):
    """
    导入 acg_bb_上期收发存结果表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "para_inventory_summary", False)
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/process_recalculate", tags=["3.3.2.2 生产物料核算"])
async def process_recalculate():
    """
    执行单价重算，加工处理以下表中信息：
    acg_dc_半成品生产收发存表
    acg_dd_消耗单价重算
    acg_de_入库单价重算
    acg_df_自制半成品转库存计价
    acg_dg_自制半成品销售计价
    """
    try:
        ba_mat_prod_service.process_recalculate()
        return ok()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_dc_semi_inventory", tags=["3.3.2.2 生产物料核算"])
async def get_acg_dc_semi_inventory():
    """
    acg_dc_半成品生产收发存表
    """
    try:
        data = db.get_table('acg_dc_半成品生产收发存表', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_dd_consume_recalculate", tags=["3.3.2.2 生产物料核算"])
async def get_acg_dd_consume_recalculate():
    """
    acg_dd_消耗单价重算
    """
    try:
        data = db.get_table('acg_dd_消耗单价重算', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_de_instorage_recalculate", tags=["3.3.2.2 生产物料核算"])
async def get_acg_de_instorage_recalculate():
    """
    acg_de_入库单价重算
    """
    try:
        data = db.get_table('acg_de_入库单价重算', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_df_semi_inventory_pricing", tags=["3.3.2.2 生产物料核算"])
async def get_acg_df_semi_inventory_pricing():
    """
    acg_df_自制半成品转库存计价
    """
    try:
        data = db.get_table('acg_df_自制半成品转库存计价', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_dg_semi_sales_pricing", tags=["3.3.2.2 生产物料核算"])
async def get_acg_dg_semi_sales_pricing():
    """
    acg_dg_自制半成品销售计价
    """
    try:
        data = db.get_table('acg_dg_自制半成品销售计价', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/process_merchandise_inventory", tags=["3.3.2.2 生产物料核算"])
async def process_merchandise_inventory():
    """
    处理库存商品相关信息，生成以下表中内容：
    acg_ea_当期库存商品收发存表
    acg_eb_库存商品销售计价
    """
    try:
        ba_mat_prod_service.process_merchandise_inventory
        return ok()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_ea_merchandise_inventory", tags=["3.3.2.2 生产物料核算"])
async def get_acg_ea_merchandise_inventory():
    """
    acg_ea_当期库存商品收发存表
    """
    try:
        data = db.get_table('acg_ea_当期库存商品收发存表', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/acg_eb_merchandise_sales", tags=["3.3.2.2 生产物料核算"])
async def acg_eb_merchandise_sales():
    """
    acg_eb_库存商品销售计价
    """
    try:
        data = db.get_table('acg_eb_库存商品销售计价', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_fa_adjust_instorage_voucher", tags=["3.3.2.2 生产物料核算"])
async def get_acg_fa_adjust_instorage_voucher():
    """
    acg_fa_调整入库凭证
    """
    try:
        ba_mat_prod_service.process_acg_fa_adjust_instorage_voucher()
        data = db.get_table('acg_fa_调整入库凭证', 1, 1000)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_fb_adjust_consume_voucher", tags=["3.3.2.2 生产物料核算"])
async def get_acg_fb_adjust_consume_voucher():
    """
    acg_fb_调整消耗凭证
    """
    try:
        ba_mat_prod_service.process_acg_fb_adjust_consume_voucher()
        data = db.get_table('acg_fb_调整消耗凭证', 1, 1000)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_fc_append_instorage_voucher", tags=["3.3.2.2 生产物料核算"])
async def get_acg_fc_append_instorage_voucher():
    """
    acg_fc_入库凭证追加行
    """
    try:
        ba_mat_prod_service.process_acg_fc_append_instorage_voucher()
        data = db.get_table('acg_fc_入库凭证追加行', 1, 1000)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_fd_main_cost_voucher", tags=["3.3.2.2 生产物料核算"])
async def get_acg_fd_main_cost_voucher():
    """
    acg_fd_调后转主营成本凭证
    """
    try:
        ba_mat_prod_service.process_acg_fd_main_cost_voucher()
        data = db.get_table('acg_fd_调后转主营成本凭证', 1, 1000)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_fe_inventory_summary", tags=["3.3.2.2 生产物料核算"])
async def get_acg_fe_inventory_summary():
    """
    acg_fe_当期收发存结果表
    """
    try:
        ba_mat_prod_service.process_acg_fe_inventory_summary()
        data = db.get_table('acg_fe_当期收发存结果表', 1, 1000)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_prod_router.get("/get_acg_ad_stat_project", tags=["3.3.2.2 生产物料核算"])
async def get_acg_ad_stat_project():
    """
    acg_ad_统计_研发项目
    """
    try:
        ba_mat_prod_service.process_acg_ad_stat_project()
        data = db.get_table('acg_ad_统计_研发项目', 1, 1000)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))
