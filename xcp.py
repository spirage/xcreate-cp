# -*- coding: UTF-8 -*-
import time

from core.database import *
import service.mat_detail_service as mat_service
import service.ba_rd_split_service as rd_service
from core.database import *


# def test_round():
#     cur = exec_query("select orig_rowno, 本币金额 from voucher_merged_test" )
#     rows = cur.fetchall()
#     for row in rows:
#         exec_command("update voucher_merged_test set 本币金额=" + str(round(row[1], 2)) + " where orig_rowno=" + str(row[0]))


if __name__ == '__main__':
    logger.info('软件开始运行')
    time_start = time.time()
    #import_table("para_voucher_summary_transfer")
    #export_table("code_cost_account")
    #get_mat_detail()
    # mat_service.gen_mat_detail()
    #mat_service.set_mat_group()
    #mat_service.fill_mat_detail()
    #service.process_recalculate()
    #fill_mat_track()
    #tag_mat_group()

    #rd_service.gen_staff_manhour_detail()

    # test_round()

    #version = conn.execute("SELECT SQLITE_VERSION()").fetchone()[0]
    # 输出版本信息
    #print("SQLite版本:", version)

    time_end = time.time()
    logger.info('运行完成，共计用时 %s 秒' % (str(round(time_end - time_start, 0))))
    conn.close()

