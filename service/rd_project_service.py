# -*- coding: UTF-8 -*-

from core.database import *


def map_project_mat():
    logger.info("建立项目和物料对应关系")
    exec_command("drop table if exists map_project_mat")
    exec_command("""
create table map_project_mat as 
select row_number()over() row_no, 项目序号 project_id, 重量临界值 critical_weight, mat_group_no, group_wt, group_start_date 
  from ( select x.mat_group_no, x.group_wt, y.项目序号, y.重量临界值, group_start_date, y."index" idx
           from ( select d.mat_group_no, group_concat(d.cost_center || ',' || d.product_code, '-') tgtj, sum(mat_wt) group_wt, min(prod_date) group_start_date
                    from mat_track_detail d 
                   where d.account_title_item = '01'
                     and d.cost_center <> ' '
                   group by d.mat_group_no ) x, 
                para_project y 
          where exists( select 1 from mat_track_detail
                         where mat_group_no = x.mat_group_no 
                           and account_title_item = '01'
                           and cost_center <> ' ' 
                           and instr(y.牌号信息,ba_object_sub_1) > 0) 
            and x.tgtj = y.通过序号1 || coalesce('-' || y.通过序号2, '') || coalesce('-' || y.通过序号3, '') || coalesce('-' || y.通过序号4, '') || coalesce('-' || y.通过序号5, '') || coalesce('-' || y.通过序号6, '') || coalesce('-' || y.通过序号7, '') || coalesce('-' || y.通过序号8, '') || coalesce('-' || y.通过序号9, '') || coalesce('-' || y.通过序号10, '') 
            and x.group_start_date >= y.开始日期) 
 order by idx, mat_group_no
    """)
    cur = exec_query("select distinct project_id, critical_weight from map_project_mat")
    for row in cur:
        # 删除本项目临界外映射关系
        exec_command("delete from map_project_mat as a " +
                     "where project_id='" + str(row[0]) + "' "
                     "  and (select sum(group_wt) from map_project_mat b where project_id='" + str(row[0]) + "' and idx<a.idx) >= " + str(row[1])
                    )
        # 删除后续项目临界内本项目用过的映射关系
        exec_command("delete from map_project_mat as a " +
                     "where row_no>(select max(row_no) from map_project_mat b where b.project_id='"+str(row[0])+"') " +
                     "  and mat_group_no in (select mat_group_no from map_project_mat b where b.project_id='"+str(row[0])+"')"
                    )
        # 更新人工参数表中匹配重量
        exec_command("update para_project as a " +
                     "set 匹配重量=(select sum(group_wt) from map_project_mat b where b.project_id=a.项目序号)")


def tag_mat_group():
    logger.info("标记创新项目")
    map_project_mat()
    exec_command("update mat_track_detail as a set project_id=(select project_id from map_project_mat b where b.mat_group_no=a.mat_group_no)")
    logger.info("标记多项标志")
    exec_command("update mat_track_detail as a set multi_object = 'Y' where exists(select 1 from mat_track_detail b where b.mat_group_no = a.mat_group_no and b.cost_center = a.cost_center and b.product_code = a.product_code and b.ba_object_sub_1 <> a.ba_object_sub_1)")
    logger.info("标记首次投入")
    exec_command("update mat_track_detail as a set first_in = 'Y' where a.project_id is not null and pass_backlog_seq_no='1' and account_title_item = '01' and not exists (select 1 from mat_track_detail b where b.mat_group_no = a.mat_group_no and b.id<>a.id)")
    exec_command("update mat_track_detail as a set first_in = 'Y' where a.project_id is not null and pass_backlog_seq_no = ( select min(pass_backlog_seq_no) from mat_track_detail b where b.mat_group_no = a.mat_group_no ) and account_title_item = '31' and exists (select 1 from mat_track_detail b where b.mat_group_no = a.mat_group_no and account_title_item = '01')")
    logger.info("标记首次产出")
    exec_command("update mat_track_detail as a set first_out = 'Y' where a.project_id is not null and pass_backlog_seq_no = ( select min(pass_backlog_seq_no) from mat_track_detail b where b.mat_group_no = a.mat_group_no ) and account_title_item = '01' and exists (select 1 from mat_track_detail b where b.mat_group_no = a.mat_group_no and account_title_item = '01')")


def process_semi_product_in_out():
    logger.info("处理半成品投入产出")
    exec_command("drop table if exists semi_product_in_out")
    exec_command("""
create table semi_product_in_out as
select right.project_id, right.mat_group_no, in_id, in_cost_center, in_product_code, in_object, in_wt, in_storage, out_id,out_cost_center, out_product_code, out_object, voucher_type, out_seq_no, out_wt 
from 

(
select a.project_id, a.mat_group_no,
       a.row_no out_row_no,
       case when a.first_out='Y' then 'N' else (case when a.cost_center<>' ' and a.account_title_item='01' then 'N' when a.cost_center=' ' and a.account_title_item='01' then 'Y' end) end as in_storage, 
       a.cost_center out_cost_center, a.product_code out_product_code, a.ba_object_sub_1 out_object, 
       case when a.first_out='Y' then '生产成本结转' else (case when a.cost_center<>' ' and a.account_title_item='01' then '入库消耗凭证' when a.cost_center=' ' and a.account_title_item='01' then '入库凭证' end) end voucher_type, 
       a.pass_backlog_seq_no out_seq_no, a.mat_wt out_wt, 
       a.account_title_item out_account_title, a.first_in 
from mat_track_detail a
where a.project_id is not null
  and (a.first_out = 'Y' or a.account_title_item ='01') --??
order by mat_group_no, pass_backlog_seq_no
) as right 

full outer join

(       
select a.project_id, a.mat_group_no,
       a.row_no in_row_no,
       a.cost_center in_cost_center, a.product_code in_product_code, a.ba_object_sub_1 in_object, 
       a.pass_backlog_seq_no in_seq_no, a.mat_wt in_wt 
from mat_track_detail a
where a.project_id is not null
  and a.account_title_item in ('30','31')
order by mat_group_no, pass_backlog_seq_no
) as left 

on  left.project_id=right.project_id and left.mat_group_no=right.mat_group_no -- and in_seq_no=out_seq_no --??
and in_cost_center=out_cost_center
    """)


def process_semi_product_in_out_sum():
    logger.info("半成品投入产出归集")
    exec_command("drop table if exists semi_product_in_out_sum")
    exec_command("""
create table semi_product_in_out_sum as
select *, case when out_product_code in (select code from code_product where name like '%-外销') then 'B 外销类'
               when in_product_code in (select source_code from code_cost_account where source_type = '直接支用-外购半成品') then 'c 外购半成品类'
               when in_product_code = out_product_code then 'D 相同产品'
               else 'A 普通类' end type
from (
select in_product_code, round(sum(in_wt),4) in_wt, group_concat(in_row_no, ',') in_rows,
       out_product_code, round(sum(out_wt),4) out_wt, group_concat(out_row_no, ',') out_rows
from semi_product_in_out
group by in_product_code, out_product_code
)
    """)


def process_map_project_product():
    logger.info("aca_ca_首次分配数据源")
    exec_command("drop table if exists map_project_product")
    exec_command("""
create table map_project_product as
select (select 项目编码 from para_project b where b.项目序号=a.project_id) project_code, 
       cost_center cost_center_code, product_code, ba_object_sub_1, round(sum(mat_wt),4) wt
from mat_track_detail a     
where a.project_id is not null
  and a.account_title_item = '01'
  and a.cost_center <> ' '
group by (select 项目编码 from para_project b where b.项目序号=a.project_id),
         cost_center, product_code, ba_object_sub_1
    """)


# def process_aca_cb():
#     logger.info("aca_cb_履历码首笔投入")
#     exec_command("drop table if exists aca_cb")
#     exec_command("""
# create table aca_cb as
# select * from mat_track_detail where account_title_item='31' and cost_center<>' '
#     """)

#def process_aca_cb():

def process_aca_db():
    logger.info("aca_db_中间试验品数据源")
    exec_command("drop table if exists aca_db")
    exec_command("""
create table aca_db as
select * from mat_track_detail 
where project_id is not null
  and mat_group_no not in (select mat_group_no from para_inventory_transfer) 
  and account_title_item='01' and cost_center<>' '
  and instr( (select group_concat(out_rows,',') from semi_product_in_out_sum where type='A 普通类'), row_no ) > 0
    """)


# 20240504 code_product 已在优化步骤统一生成
# def process_code_product():
#     logger.info("生成code_product代码表")
#     exec_command("drop table if exists code_product")
#     exec_command("create table code_product as select distinct 账务代码 code, 账务代码名称 name, null transfer, null cost_center_code from aca_u_x where 类型='产副品对应'")
#     exec_command("update code_product as a set transfer = (select code from code_product b where b.name = substr(a.name, 1, instr(a.name,'-外销')-1))")
#     exec_command("""
# update code_product as a
# set cost_center_code = (
# select cost_center_code from
# (select distinct substr(产副品代码,1,instr(产副品代码,'-')-1) product_code, substr(成本中心,1,instr(成本中心,'-')-1) cost_center_code  from orig_product_cost) b
# where b.product_code = a.code)
#     """)


def process_aca_dc():
    logger.info("aca_dc_转库存履历码产出明细")
    # process_code_product()
    exec_command("drop table if exists aca_dc")
    exec_command("""
create table aca_dc as
select a.row_no, a.mat_group_no, a.mat_track_no, a.mat_no, a.in_mat_no, pass_backlog_seq_no, whole_backlog_code, whole_backlog_name, 
       d.cost_center_code cost_center, c.transfer product_code, ba_object_sub_1, account_title_item, prod_date, event_related_id, mat_wt, project_id, first_in, first_out  
from mat_track_detail a,
     para_inventory_transfer b,
     code_product c,
     code_product d
where a.mat_group_no = b.mat_group_no
  and a.product_code = c.code
  and c.transfer is not null
  and c.transfer = d.code
  and a.account_title_item='01' 
  and a.cost_center=' '
    """)


def process_aca_dd():
    logger.info("aca_dd_转库存履历码产出归集")
    exec_command("drop table if exists aca_dd")
    exec_command("""
create table aca_dd as
select cost_center, product_code, round(sum(mat_wt),4) mat_wt
from aca_dc
group by 1,2
    """)
