# -*- coding: UTF-8 -*-
from typing import Union
from fastapi import UploadFile, File
from pydantic import BaseModel
from starlette.responses import Response

from service.ba_rd_split_service import *
from core.response import *
from fastapi.routing import APIRouter
import service.ba_rd_split_service as ba_rd_split_service
import core.database as db

ba_rd_split_router = APIRouter()


class Page(BaseModel):
    page: Union[int, None] = 1
    limit: Union[int, None] = 500


@ba_rd_split_router.get("/tpl_voucher_entry", tags=["3.3.1 研发投入核算"])
async def tpl_voucher_entry():
    """
    获取原始凭证分录表模板
    """
    try:
        file_for_download = "orig_voucher_entry.xlsx"
        excel_content = db.export_through_mem("tpl_voucher_entry")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.post("/imp_voucher_entry", tags=["3.3.1 研发投入核算"])
async def imp_voucher_entry(file: UploadFile = File(...)):
    """
    导入原始凭证分录表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "orig_voucher_entry")
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/gen_voucher_entry", tags=["3.3.1 研发投入核算"])
async def gen_voucher_entry():
    """
    <b>acc_凭证表加工_1生成</b> <br>
     20240615 凭证表加工 改名为 财务信息维护
    """
    try:
        ba_rd_split_service.gen_voucher_entry()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))
    return ok()


@ba_rd_split_router.get("/tag_voucher_entry", tags=["3.3.1 研发投入核算"])
async def tag_voucher_entry():
    """
    <b>acc_凭证表加工_2标记</b> <br>
     20240615 凭证表加工 改名为 财务信息维护
    """
    try:
        ba_rd_split_service.tag_voucher_entry()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))
    return ok()


@ba_rd_split_router.post("/get_voucher_entry", tags=["3.3.1 研发投入核算"])
async def get_voucher_entry(page: Page):
    """
    <b>acc_凭证表加工_3获取</b> <br>
     20240615 凭证表加工 改名为 财务信息维护
    """
    try:
        data = db.get_table('voucher_entry', page.page, page.limit)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/tpl_para_reimbursement_preparation", tags=["3.3.1 研发投入核算"])
async def tpl_para_reimbursement_preparation():
    """
    获取需跳过（报制单类）的成本科目人工设置表模板
    """
    try:
        file_for_download = "para_reimbursement_preparation.xlsx"
        excel_content = db.export_through_mem("tpl_para_reimbursement_preparation")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.post("/imp_para_reimbursement_preparation", tags=["3.3.1 研发投入核算"])
async def imp_para_reimbursement_preparation(file: UploadFile = File(...)):
    """
    导入需跳过（报制单类）的成本科目人工设置表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "para_reimbursement_preparation", False)
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/tpl_para_caccount_raccount", tags=["3.3.1 研发投入核算"])
async def tpl_para_caccount_raccount():
    """
    获取成本科目与研发科目对应人工设置表模板
    """
    try:
        file_for_download = "para_caccount_raccount.xlsx"
        excel_content = db.export_through_mem("tpl_para_caccount_raccount")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.post("/imp_para_caccount_raccount", tags=["3.3.1 研发投入核算"])
async def imp_para_caccount_raccount(file: UploadFile = File(...)):
    """
    导入成本科目与研发科目对应人工设置表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "para_caccount_raccount", False)
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/tpl_para_project_capitalized_cost", tags=["3.3.1 研发投入核算"])
async def tpl_para_project_capitalized_cost():
    """
    获取项目资本化设置人工设置表模板
    """
    try:
        file_for_download = "para_project_capitalized_cost.xlsx"
        excel_content = db.export_through_mem("tpl_para_project_capitalized_cost")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.post("/imp_para_project_capitalized_cost", tags=["3.3.1 研发投入核算"])
async def imp_para_project_capitalized_cost(file: UploadFile = File(...)):
    """
    导入项目资本化设置人工设置表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "para_project_capitalized_cost", False)
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/process_map_ccenter_caccount_svoucher", tags=["3.3.1 研发投入核算"])
async def process_map_ccenter_caccount_svoucher():
    """
    acc_产副品表与凭证表分析 1 处理
    """
    try:
        ba_rd_split_service.process_map_ccenter_caccount_svoucher()
        return ok()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/get_map_ccenter_caccount_svoucher", tags=["3.3.1 研发投入核算"])
async def get_map_ccenter_caccount_svoucher():
    """
    acc_产副品表与凭证表分析 2 获取
    """
    try:
        data = db.get_table('map_ccenter_caccount_svoucher', 1, 1000)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


class VoucherIndex(BaseModel):
    voucher_index: Union[int, None] = -1


@ba_rd_split_router.post("/get_voucher_detail", tags=["3.3.1 研发投入核算"])
async def get_voucher_detail(voucher_index: VoucherIndex):
    """
    通过凭证index获取凭证详情
    """
    try:
        data = ba_rd_split_service.get_voucher_detail(voucher_index.voucher_index)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


class VoucherNo(BaseModel):
    voucher_no: Union[str, None] = None


@ba_rd_split_router.post("/get_voucher_by_no", tags=["3.3.1 研发投入核算"])
async def get_voucher_by_no(voucher_no: VoucherNo):
    """
    通过凭证号码获取凭证信息
    """
    try:
        data = ba_rd_split_service.get_voucher_by_no(voucher_no.voucher_no)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


class VoucherSelection(BaseModel):
    rowno: Union[int, None] = -1
    voucher_index: Union[int, None] = -1


@ba_rd_split_router.post("/select_voucher", tags=["3.3.1 研发投入核算"])
async def select_voucher(voucher_selection: VoucherSelection):
    """
    acc_产副品表与凭证表分析 3 选择凭证
    <br>post参数格式：
    { "rowno": 在产副品表与凭证表分析表中的rowno, "voucher_index": 选择的凭证index号 }
    """
    try:
        pass
        ba_rd_split_service.select_voucher(voucher_selection.rowno, voucher_selection.voucher_index)
        data = db.get_table('map_ccenter_caccount_svoucher', 1, 1000)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/exp_para_ba_rd_split_ratio", tags=["3.3.1 研发投入核算"])
async def exp_para_ba_rd_split_ratio():
    """
    导出当前研发投入核算系数
    """
    try:
        ba_rd_split_service.gen_para_ba_rd_split_ratio()
        file_for_download = "para_ba_rd_split_ratio.xlsx"
        excel_content = db.export_through_mem("para_ba_rd_split_ratio")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.post("/imp_para_ba_rd_split_ratio", tags=["3.3.1 研发投入核算"])
async def imp_para_ba_rd_split_ratio(file: UploadFile = File(...)):
    """
    导入研发投入核算系数人工设置表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "para_ba_rd_split_ratio", False)
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/split_voucher", tags=["3.3.1 研发投入核算"])
async def split_voucher():
    """
    acc_处理拆分凭证
    """
    try:
        ba_rd_split_service.process_voucher_splitted()
        return ok()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.post("/get_voucher_splitted", tags=["3.3.1 研发投入核算"])
async def get_voucher_splitted(page: Page):
    """
    acc_获取拆分后凭证
    """
    try:
        data = db.get_table('voucher_splitted', page.page, page.limit)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


# 0704 新增 王坤群里联系增加成本科目和研发科目保留首次分配时数据需求
@ba_rd_split_router.get("/process_stat_raccount_orig", tags=["3.3.1 研发投入核算"])
async def process_stat_raccount_orig():
    """
    处理 初始 acc_统计研发科目 首次分配时调用
    """
    try:
        ba_rd_split_service.process_stat_raccount_orig()
        return ok()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


# 0704 新增 王坤群里联系增加成本科目和研发科目保留首次分配时数据需求
@ba_rd_split_router.get("/get_stat_raccount_orig", tags=["3.3.1 研发投入核算"])
async def get_stat_raccount_orig():
    """
    获取 初始 acc_统计研发科目 首次分配时调用
    """
    try:
        data = db.get_table('stat_raccount_orig', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/get_stat_raccount", tags=["3.3.1 研发投入核算"])
async def get_stat_raccount():
    """
    acc_统计研发科目
    """
    try:
        ba_rd_split_service.process_stat_raccount()
        data = db.get_table('stat_raccount', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/process_stat_project_orig", tags=["3.3.1 研发投入核算"])
async def process_stat_project_orig():
    """
    处理 初始 acc_统计_研发项目 首次分配时调用
    """
    try:
        ba_rd_split_service.process_stat_project_orig()
        return ok()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/get_stat_project_orig", tags=["3.3.1 研发投入核算"])
async def get_stat_project_orig():
    """
    获取 初始 acc_统计_研发项目 首次分配时调用
    """
    try:
        data = db.get_table('stat_project_orig', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/get_stat_project", tags=["3.3.1 研发投入核算"])
async def get_stat_project():
    """
    acc_统计_研发项目 导入系数人工设置数据后调用 多次分配
    """
    try:
        ba_rd_split_service.process_stat_project()
        data = db.get_table('stat_project', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


# 0704 新增 王坤群里联系增加成本科目和研发科目保留首次分配时数据需求
@ba_rd_split_router.get("/process_stat_caccount_orig", tags=["3.3.1 研发投入核算"])
async def process_stat_caccount_orig():
    """
    处理 初始 acc_统计_成本元素 首次分配时调用
    """
    try:
        ba_rd_split_service.process_stat_caccount_orig()
        return ok()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


# 0704 新增 王坤群里联系增加成本科目和研发科目保留首次分配时数据需求
@ba_rd_split_router.get("/get_stat_caccount_orig", tags=["3.3.1 研发投入核算"])
async def get_stat_caccount_orig():
    """
    获取 初始 acc_统计_成本元素 首次分配时调用
    """
    try:
        data = db.get_table('stat_caccount_orig', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/get_stat_caccount", tags=["3.3.1 研发投入核算"])
async def get_stat_caccount():
    """
    acc_统计_成本元素 导入系数人工设置数据后调用 多次分配
    """
    try:
        ba_rd_split_service.process_stat_caccount()
        data = db.get_table('stat_caccount', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/tpl_para_recycle", tags=["3.3.1 研发投入核算"])
async def tpl_para_recycle():
    """
    acd_系数设置后指标统计_左 中 回收参数 人工设置参数参数表模板
    <br>cost_center_code和product_code为all的行ratio_recycle(回收比)为I5单元格回收比
    <br>ratio_recycle 回收比
    <br>ratio_discount 折价比
    <br>ratio_adjust 研发产品单价系数
    """
    try:
        file_for_download = "para_recycle.xlsx"
        excel_content = db.export_through_mem("tpl_para_recycle")
        response = Response(content=excel_content, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response.headers['Content-Disposition'] = 'attachment; filename=' + file_for_download
        return response
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.post("/imp_para_recycle", tags=["3.3.1 研发投入核算"])
async def imp_para_recycle(file: UploadFile = File(...)):
    """
    导入acd_系数设置后指标统计_左 中 回收参数 人工设置参数参数表
    """
    try:
        if file.content_type in ("application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            await db.import_through_mem(file, "para_recycle", False)
            return ok()
        else:
            return fail(415, "文件类型错误，请上传Excel xlsx 或 xls格式")
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/get_stat_after_adjusted_left", tags=["3.3.1 研发投入核算"])
async def get_stat_after_adjusted_left():
    """
    acd_系数设置后指标统计_左
    """
    try:
        ba_rd_split_service.process_stat_after_adjusted_left()
        data = db.get_table('acd_系数设置后指标统计_左', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/get_stat_after_adjusted_right", tags=["3.3.1 研发投入核算"])
async def get_stat_after_adjusted_right():
    """
    acd_系数设置后指标统计_右
    """
    try:
        ba_rd_split_service.process_stat_after_adjusted_right()
        data = db.get_table('acd_系数设置后指标统计_右', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_rd_split_router.get("/get_stat_voucher_balance", tags=["3.3.1 研发投入核算"])
async def get_stat_voucher_balance():
    """
    acd_值状态测试结果
    """
    try:
        ba_rd_split_service.process_stat_voucher_balance()
        data = db.get_table('stat_voucher_balance', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))

