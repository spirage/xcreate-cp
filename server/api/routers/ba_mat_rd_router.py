# -*- coding: UTF-8 -*-
from typing import Union
from pydantic import BaseModel
from service.ba_mat_rd_service import *
from core.response import *
from fastapi.routing import APIRouter
import service.ba_mat_rd_service as ba_mat_rd_service
import core.database as db

ba_mat_rd_router = APIRouter()


class Page(BaseModel):
    page: Union[int, None] = 1
    limit: Union[int, None] = 500


@ba_mat_rd_router.get("/get_ace_ba_bb", tags=["3.3.2.1 研发物料核算"])
async def get_ace_ba_bb():
    """
    ace_ba_中间试验品回收比例
    ace_bb_中间试验品回收分配
    """
    try:
        ba_mat_rd_service.process_ace_ba_bb()
        data = db.get_table('ace_ba_bb', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_ace_bc", tags=["3.3.2.1 研发物料核算"])
async def get_ace_bc():
    """
    ace_bc_中间试验品回收明细
    """
    try:
        ba_mat_rd_service.process_ace_bc()
        data = db.get_table('ace_bc', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_ace_ca_cb", tags=["3.3.2.1 研发物料核算"])
async def get_ace_ca_cb():
    """
    ace_ca_研发产品入库比例
    ace_cb_研发产品入库分配
    """
    try:
        ba_mat_rd_service.process_ace_ca_cb()
        data = db.get_table('ace_ca_cb', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_ace_cc", tags=["3.3.2.1 研发物料核算"])
async def get_ace_cc():
    """
    ace_cc_研发产品入库明细
    """
    try:
        ba_mat_rd_service.process_ace_cc()
        data = db.get_table('ace_cc', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))

# 20240505 去除 code_caccount_transfer，已在优化步骤统一生成code_cost_account
# @ba_mat_rd_router.get("/get_code_caccount_transfer", tags=["3.3.2.1 研发物料核算"])
# async def get_code_caccount_transfer():
#     """
#     acf_后台表_u_x 类型='成本科目来源'
#     """
#     try:
#         ba_mat_rd_service.process_code_caccount_transfer()
#         data = db.get_table('code_caccount_transfer', 1, 500)
#         return ok(data)
#     except sqlite3.OperationalError as oe:
#         logger.error("数据库操作异常：" + str(oe))
#         return fail(21, "数据库操作异常：" + str(oe))
#     except Exception as ex:
#         logger.error(ex)
#         return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_ad_voucher_instorage_consume", tags=["3.3.2.1 研发物料核算"])
async def get_acf_ad_voucher_instorage_consume():
    """
    acf_ad_初始凭证_入库消耗
    """
    try:
        ba_mat_rd_service.process_acf_ad_voucher_instorage_consume()
        data = db.get_table('acf_ad_初始凭证_入库消耗', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_ad_splitted_instorage_consume", tags=["3.3.2.1 研发物料核算"])
async def get_acf_ad_splitted_instorage_consume():
    """
    acf_ad_分配后入库消耗
    """
    try:
        ba_mat_rd_service.process_acf_ad_splitted_instorage_consume()
        data = db.get_table('acf_ad_分配后入库消耗', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


# 0618 新增 将原本在 get_acf_bb_consume_orig 和 get_acf_ba_instorage_orig 两个接口中 处理数据的部分 统一放至本接口执行
@ba_mat_rd_router.get("/process_orig_consume_instorage", tags=["3.3.2.1 研发物料核算"])
async def process_orig_consume_instorage():
    """
    <b>处理初始消耗凭证和初始入库凭证接口</b> <br>
    将原本在 get_acf_bb_consume_orig 和 get_acf_ba_instorage_orig 两个接口中 处理数据的部分 统一放至本接口执行 <br>
    get_acf_bb_consume_orig 和 get_acf_ba_instorage_orig 保留获取数据的功能
    """
    try:
        ba_mat_rd_service.process_acf_bb_consume_orig()
        ba_mat_rd_service.process_acf_ba_instorage_orig()
        return ok()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_bb_consume_orig", tags=["3.3.2.1 研发物料核算"])
async def get_acf_bb_consume_orig():
    """
    <b>获取 acf_bb_初始凭证-消耗</b>
    """
    try:
        data = db.get_table('acf_bb_consume_orig', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_ba_instorage_orig", tags=["3.3.2.1 研发物料核算"])
async def get_acf_ba_instorage_orig():
    """
    <b>获取 acf_ba_初始凭证-入库</b>
    """
    try:
        data = db.get_table('acf_ba_instorage_orig', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


# 0618 新增 入库转消耗接口 和 消耗转入库接口
class Index(BaseModel):
    index: Union[int, None] = -1


@ba_mat_rd_router.post("/transfer_instorage_to_consume", tags=["3.3.2.1 研发物料核算"])
async def transfer_instorage_to_consume(index: Index):
    """
    <b>入库转消耗接口</b> <br>
    在 acf_ba_初始凭证-入库页面点击 转消耗 按钮后调用本接口 <br>
    将 acf_ba_初始凭证-入库 中凭证通过 凭证索引号 转入 acf_bb_初始凭证-消耗 <br>
    执行完本接口后可以调用 get_acf_ba_instorage_orig 接口查看当前初始入库凭证数据
    """
    try:
        ba_mat_rd_service.transfer_instorage_to_consume(index.index)
        return ok()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.post("/transfer_consume_to_instorage", tags=["3.3.2.1 研发物料核算"])
async def transfer_consume_to_instorage(index: Index):
    """
    <b>消耗转入库接口</b> <br>
    在 acf_bb_初始凭证-消耗 页面点击 转入库 按钮后调用本接口 <br>
    将 acf_bb_初始凭证-消耗 中凭证通过 凭证索引号 转入 acf_ba_初始凭证-入库 <br>
    执行完本接口后可以调用 get_acf_bb_consume_orig 接口查看当前初始消耗凭证数据
    """
    try:
        ba_mat_rd_service.transfer_consume_to_instorage(index.index)
        return ok()
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_bc_instorage_sum", tags=["3.3.2.1 研发物料核算"])
async def get_acf_bc_instorage_sum():
    """
    acf_bc_同项合并_入库凭证
    """
    try:
        ba_mat_rd_service.process_acf_bc_instorage_sum()
        data = db.get_table('acf_bc_instorage_sum', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_bd_consume_sum", tags=["3.3.2.1 研发物料核算"])
async def get_acf_bd_consume_sum():
    """
    acf_bd_同项合并_消耗凭证
    """
    try:
        ba_mat_rd_service.process_acf_bd_consume_sum()
        data = db.get_table('acf_bd_consume_sum', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_ca_instorage_horizontal", tags=["3.3.2.1 研发物料核算"])
async def get_acf_ca_instorage_horizontal():
    """
    acf_ca_入库借贷对照分析
    """
    try:
        ba_mat_rd_service.process_acf_ca_instorage_horizontal()
        data = db.get_table('acf_ca_instorage_horizontal', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_cb_instorage_flow", tags=["3.3.2.1 研发物料核算"])
async def get_acf_cb_instorage_flow():
    """
    acf_cb_流程顺序分析
    """
    try:
        ba_mat_rd_service.process_acf_cb_instorage_flow()
        data = db.get_table('acf_cb_instorage_flow', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_cc_consume_horizontal", tags=["3.3.2.1 研发物料核算"])
async def get_acf_cc_consume_horizontal():
    """
    acf_cc_消耗借贷对照分析
    """
    try:
        ba_mat_rd_service.process_acf_cc_consume_horizontal()
        data = db.get_table('acf_cc_consume_horizontal', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_da_instorage_adjust", tags=["3.3.2.1 研发物料核算"])
async def get_acf_da_instorage_adjust():
    """
    acf_da_调整入库凭证
    """
    try:
        ba_mat_rd_service.process_acf_da_instorage_adjust()
        data = db.get_table('acf_da_instorage_adjust', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))


@ba_mat_rd_router.get("/get_acf_db_consume_adjust", tags=["3.3.2.1 研发物料核算"])
async def get_acf_db_consume_adjust():
    """
    acf_db_调整消耗凭证
    """
    try:
        ba_mat_rd_service.process_acf_db_consume_adjust()
        data = db.get_table('acf_db_consume_adjust', 1, 500)
        return ok(data)
    except sqlite3.OperationalError as oe:
        logger.error("数据库操作异常：" + str(oe))
        return fail(21, "数据库操作异常：" + str(oe))
    except Exception as ex:
        logger.error(ex)
        return fail(24, str(ex))