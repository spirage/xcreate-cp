# -*- coding: UTF-8 -*-

from core.database import *
import service.ba_mat_prod_service as service


if __name__ == '__main__':
    logger.info('软件开始运行')
    time_start = time.time()
    #import_table("para_voucher_summary_transfer")
    export_table("code_cost_account")
    #get_mat_detail()
    #mat_service.gen_mat_detail()
    service.process_recalculate()
    #fill_mat_track()
    #tag_mat_group()

    #version = conn.execute("SELECT SQLITE_VERSION()").fetchone()[0]
    # 输出版本信息
    #print("SQLite版本:", version)

    time_end = time.time()
    logger.info('运行完成，共计用时 %s 秒' % (str(round(time_end - time_start, 0))))
    conn.close()

