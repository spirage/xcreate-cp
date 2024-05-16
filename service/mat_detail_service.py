# -*- coding: UTF-8 -*-

from core.database import *


def gen_mat_detail():
    logger.info("生成物料详情信息")
    exec_command("create index if not exists ix_orig_trans_record_event_related_id on orig_trans_record(event_related_id)")
    exec_command("create index if not exists ix_orig_trans_record_account_title_item on orig_trans_record(account_title_item)")
    exec_command("create index if not exists ix_orig_trans_record_cost_center on orig_trans_record(cost_center)")
    exec_command("create index if not exists ix_orig_trans_record_product_code on orig_trans_record(product_code)")

    exec_command("drop table if exists tmp_mat_track_detail")
    exec_command("""
      create table tmp_mat_track_detail as 
      select row_number() over() as row_no, 0 as mat_group_no, *, null project_id, null multi_object, null first_in, null first_out
      from ( select a.mat_track_no, a.mat_no, a.in_mat_no, a.pass_backlog_seq_no, a.whole_backlog_code, a.whole_backlog_name,
                    b.cost_center, b.product_code, b.ba_object_sub_1, b.account_title_item, b.prod_date, b.event_related_id, round(sum(b.mat_wt),4) as mat_wt
               from orig_mat_track a, orig_trans_record b 
              where a.mat_no = b.mat_no
              group by a.mat_track_no, a.mat_no, a.in_mat_no, a.pass_backlog_seq_no, a.whole_backlog_code, a.whole_backlog_name, 
                       b.cost_center, b.product_code, b.ba_object_sub_1, b.account_title_item, b.prod_date, b.event_related_id
              order by a.mat_track_no, a.mat_no, a.pass_backlog_seq_no, b."index" )
    """)


def set_mat_group():
    logger.info("设置物料详情分组")
    exec_command("create index if not exists ix_tmp_mat_track_detail_row_no on tmp_mat_track_detail(row_no)")
    exec_command("delete from tmp_mat_track_detail where mat_group_no<0")
    exec_command("update tmp_mat_track_detail set mat_group_no=0")
    cur = exec_query("select row_no, mat_group_no, mat_track_no, mat_no from tmp_mat_track_detail")
    mat_group_no = 1
    last_mat_track_no = ""
    for row in cur:
        if row[0] == 1:
            exec_command("update tmp_mat_track_detail set mat_group_no=" + str(mat_group_no) + " where row_no=" + str(row[0]))
            exec_command("update tmp_mat_track_detail set mat_group_no=" + str(mat_group_no) + " where mat_group_no=0 and row_no>" + str(row[0]) + " and mat_no='" + row[3] + "'")
            last_mat_track_no = row[2]
        elif row[1] == 0:
            if row[2] == last_mat_track_no:
                exec_command("update tmp_mat_track_detail set mat_group_no=" + str(mat_group_no) + " where mat_group_no=0 and row_no=" + str(row[0]))
                exec_command("update tmp_mat_track_detail set mat_group_no=" + str(mat_group_no) + " where mat_group_no=0 and row_no>" + str(row[0]) + " and mat_no='" + row[3] + "'")
            else:
                mat_group_no += 1
                exec_command("update tmp_mat_track_detail set mat_group_no=" + str(mat_group_no) + " where mat_group_no=0 and row_no=" + str(row[0]))
                last_mat_track_no = row[2]


def fill_mat_detail():
    logger.info("补充物料详情信息")
    cur = exec_query("""
            select mat_group_no, mat_no, pass_backlog_seq_no, event_related_id, mat_wt
              from ( select mat_group_no, mat_no, pass_backlog_seq_no, event_related_id, ACCOUNT_TITLE_ITEM, mat_wt,
                            row_number() over(partition by mat_group_no, pass_backlog_seq_no order by ACCOUNT_TITLE_ITEM desc) rn
                       from tmp_mat_track_detail
                   )
             where account_title_item = '31' and rn = 1
          """)
    for row in cur:
        new_pass_backlog_seq_no = str(int(row[2]) + 1)
        exec_command("insert into tmp_mat_track_detail " +
                     "select * from ( " +
                     "select -1 , " + str(row[0]) + ", 'SJH:'||event_related_id, null, '" + str(row[1]) + "', '" + new_pass_backlog_seq_no + "', whole_backlog_code, null, cost_center , product_code, ba_object_sub_1, account_title_item, max(prod_date), event_related_id, min(" + str(row[4]) + ", round(sum(mat_wt),4)) mat_wt, null, null, null, null " +
                     " from orig_trans_record " +
                     "where event_related_id = '" + row[3] + "' " +
                     "  and account_title_item in ('01', '30')" +
                     "  and (cost_center = ' ' or product_code not in ('1H021', '1K013', '1K014')) " +
                     "group by whole_backlog_code, cost_center, product_code, ba_object_sub_1, account_title_item, event_related_id " +
                     ") " +
                     "where mat_wt<>0 " +
                     "order by cost_center desc, account_title_item desc"
                     )
    exec_command("drop index if exists ix_tmp_mat_track_detail_idx")
    exec_command("alter table tmp_mat_track_detail drop column idx")
    exec_command("drop table if exists mat_track_detail")
    exec_command("""
create table mat_track_detail as 
select row_number() over(order by mat_group_no, pass_backlog_seq_no) as row_no,* 
  from tmp_mat_track_detail order by mat_group_no, pass_backlog_seq_no
    """)
    exec_command("drop table if exists tmp_mat_track_detail")
    exec_command("create index ix_mat_track_detail_cost_center on mat_track_detail(cost_center)")
    exec_command("create index ix_mat_track_detail_account_title_item on mat_track_detail(account_title_item)")
    exec_command("create index ix_mat_track_detail_mat_group_no on mat_track_detail(mat_group_no)")

