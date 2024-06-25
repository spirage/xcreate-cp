# -*- coding: UTF-8 -*-

from core.database import *


def gen_mat_detail():
    logger.info("生成物料详情信息")
    exec_command("create index if not exists ix_orig_trans_record_event_related_id on orig_trans_record(event_related_id)")
    exec_command("create index if not exists ix_orig_trans_record_account_title_item on orig_trans_record(account_title_item)")
    exec_command("create index if not exists ix_orig_trans_record_cost_center on orig_trans_record(cost_center)")
    exec_command("create index if not exists ix_orig_trans_record_product_code on orig_trans_record(product_code)")
    exec_command("create index if not exists ix_orig_trans_record_mat_no on orig_trans_record(mat_no)")
    exec_command("create index if not exists ix_orig_mat_track_mat_no on orig_mat_track(mat_no)")

    exec_command("drop table if exists tmp_mat_track_detail")
    # 0618 增加入库标识 project_id改为project_code
    exec_command("""
      create table tmp_mat_track_detail as 
      select row_number() over() as row_no, dense_rank() over(order by mat_track_no) as mat_group_no, *, null project_code, null multi_object, null first_in, null first_out, null in_storage
      from ( select a.mat_track_no, a.mat_no, a.in_mat_no, a.pass_backlog_seq_no, a.whole_backlog_code, a.whole_backlog_name,
                    b.cost_center, b.product_code, b.ba_object_sub_1, b.account_title_item, b.prod_date, b.event_related_id, round(sum(b.mat_wt),4) as mat_wt
               from orig_mat_track a, orig_trans_record b 
              where a.mat_no = b.mat_no
              group by a.mat_track_no, a.mat_no, a.in_mat_no, a.pass_backlog_seq_no, a.whole_backlog_code, a.whole_backlog_name, 
                       b.cost_center, b.product_code, b.ba_object_sub_1, b.account_title_item, b.prod_date, b.event_related_id
              order by a.mat_track_no, a.mat_no, a.pass_backlog_seq_no, b."index" )
    """)

    # 20240517 去掉set_mat_detail接口，将相关处理过程优化后直接放入gen_mat_detail
    exec_command("create index ix_tmp_mat_track_detail_row_no on tmp_mat_track_detail(row_no)")
    exec_command("create index ix_tmp_mat_track_detail_mat_no on tmp_mat_track_detail(mat_no)")
    exec_command("create index ix_tmp_mat_track_detail_in_mat_no on tmp_mat_track_detail(in_mat_no)")
    exec_command("create index ix_tmp_mat_track_detail_mat_track_no on tmp_mat_track_detail(mat_track_no)")
    exec_command("create index ix_tmp_mat_track_detail_account_title_item on tmp_mat_track_detail(account_title_item)")

    exec_command("""
update tmp_mat_track_detail as a
   set mat_group_no = (select mat_group_no from tmp_mat_track_detail b where b.row_no<a.row_no and b.mat_group_no<>a.mat_group_no and b.mat_no=a.in_mat_no limit 1)
 where exists (select 1 from tmp_mat_track_detail b where b.row_no<a.row_no and b.mat_track_no<>a.mat_track_no and b.mat_no=a.in_mat_no)
    """)

    # 20240519 去掉fill_mat_detail接口，将相关处理过程优化后直接放入gen_mat_detail
    exec_command("""
insert into tmp_mat_track_detail 
select * from ( 
select -1 , b.mat_group_no, 'SJH:'||a.event_related_id, null, b.mat_no, b.pass_backlog_seq_no+1, a.whole_backlog_code, null, a.cost_center , a.product_code, a.ba_object_sub_1, a.account_title_item, max(a.prod_date), a.event_related_id, min(b.mat_wt, round(sum(a.mat_wt),4)) mat_wt, null, null, null, null, null
 from orig_trans_record a,
      (
        select mat_group_no, mat_no, pass_backlog_seq_no, event_related_id, mat_wt
          from ( select mat_group_no, mat_no, pass_backlog_seq_no, event_related_id, ACCOUNT_TITLE_ITEM, mat_wt,
                 row_number() over(partition by mat_group_no, pass_backlog_seq_no order by ACCOUNT_TITLE_ITEM desc) rn
                 from tmp_mat_track_detail
               )
         where account_title_item = '31' and rn = 1
      ) b      
where a.event_related_id = b.event_related_id 
  and a.account_title_item in ('01', '30')
  and (a.cost_center = ' ' or a.product_code not in ('1H021', '1K013', '1K014')) 
group by b.mat_group_no, a.event_related_id, b.mat_no, b.pass_backlog_seq_no, a.whole_backlog_code, a.cost_center, a.product_code, a.ba_object_sub_1, a.account_title_item 
) 
where mat_wt<>0 
order by cost_center desc, account_title_item desc
    
    """)
    exec_command("drop index if exists ix_tmp_mat_track_detail_row_no")
    exec_command("alter table tmp_mat_track_detail drop column row_no")
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

