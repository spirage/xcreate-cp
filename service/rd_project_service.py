# -*- coding: UTF-8 -*-

from core.database import *


# 0618 project_id 改为 project_code
def map_project_mat():
    logger.info("建立项目和物料对应关系")
    exec_command("drop table if exists map_project_mat")
    exec_command("""
create table map_project_mat as 
select row_number()over() row_no, "index" project_index, 项目编码 project_code, 重量临界值 critical_weight, mat_group_no, group_wt, group_start_date 
  from ( select x.mat_group_no, x.group_wt, y."index", y.项目编码, y.重量临界值, group_start_date, y."index" idx
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
    cur = exec_query("select distinct project_index, critical_weight from map_project_mat")
    for row in cur:
        # 删除本项目临界外映射关系
        exec_command("delete from map_project_mat as a " +
                     "where project_index='" + str(row[0]) + "' "
                     "  and (select sum(group_wt) from map_project_mat b where b.project_index='" + str(row[0]) + "' and b.row_no<a.row_no) >= " + str(row[1])
                    )
        # 删除后续项目临界内本项目用过的映射关系
        exec_command("delete from map_project_mat as a " +
                     "where a.row_no>(select max(b.row_no) from map_project_mat b where b.project_index='"+str(row[0])+"') " +
                     "  and a.mat_group_no in (select b.mat_group_no from map_project_mat b where b.project_index='"+str(row[0])+"')"
                    )
        # 更新人工参数表中匹配重量
        exec_command("update para_project as a " +
                     "set 匹配重量=(select sum(b.group_wt) from map_project_mat b where b.project_index=a.\"index\")")


# 0618 project_id 改为 project_code
def tag_mat_group():
    logger.info("标记创新项目")
    map_project_mat()
    exec_command("update mat_track_detail as a set project_code=(select project_code from map_project_mat b where b.mat_group_no=a.mat_group_no)")
    logger.info("标记多项标志")
    exec_command("update mat_track_detail as a set multi_object = 'Y' where a.account_title_item = '01' and exists(select 1 from mat_track_detail b where b.mat_group_no = a.mat_group_no and b.cost_center = a.cost_center and b.product_code = a.product_code and b.ba_object_sub_1 <> a.ba_object_sub_1 and b.account_title_item = '01')")
    logger.info("标记首次投入")
    exec_command("update mat_track_detail as a set first_in = 'Y' where a.project_code is not null and pass_backlog_seq_no='1' and account_title_item = '01' and not exists (select 1 from mat_track_detail b where b.mat_group_no = a.mat_group_no and b.row_no<>a.row_no)")
    exec_command("update mat_track_detail as a set first_in = 'Y' where a.project_code is not null and pass_backlog_seq_no = ( select min(pass_backlog_seq_no) from mat_track_detail b where b.mat_group_no = a.mat_group_no ) and account_title_item = '31' and exists (select 1 from mat_track_detail b where b.mat_group_no = a.mat_group_no and account_title_item = '01')")
    logger.info("标记首次产出")
    exec_command("update mat_track_detail as a set first_out = 'Y' where a.project_code is not null and pass_backlog_seq_no = ( select min(pass_backlog_seq_no) from mat_track_detail b where b.mat_group_no = a.mat_group_no ) and account_title_item = '01' and exists (select 1 from mat_track_detail b where b.mat_group_no = a.mat_group_no and account_title_item = '01')")


# 根据0525沟通记录 0531增加
def process_stat_project_wt():
    logger.info("处理研发项目匹配结果统计表")
    exec_command("drop table if exists stat_project_wt")
    exec_command("""
create table stat_project_wt as 
select 项目编码, 项目名称, sum(重量临界值) 项目设置重量, sum(匹配重量) 项目匹配重量
from para_project
group by  1, 2
    """)


def fill_code_product():
    logger.info("导入orig_product_cost时补充code_product")
    # 0626 新增 第2种情况，即 产品代码表中已存在代码，但本月产副品成本构成表中名称与代码表中不一样 情况的处理
    # 1 产品代码表中不存在，本月产副品成本构成表中新出现的代码及相关信息，添加到产品代码表
    exec_command("""    
insert into code_product
select distinct substr(产副品代码,1,instr(产副品代码,'-')-1), substr(产副品代码,instr(产副品代码,'-')+1), null, substr(成本中心,1,instr(成本中心,'-')-1)
from orig_product_cost
where substr(产副品代码,1,instr(产副品代码,'-')-1) not in (select code from code_product)
    """)
    # 2 产品代码表中已存在代码，但本月产副品成本构成表中名称与代码表中不一样，按照成本构成表中名称更新产品代码表
    exec_command("""
update code_product as a 
set name = (select substr(产副品代码,instr(产副品代码,'-')+1) from orig_product_cost b where substr(b.产副品代码,1,instr(b.产副品代码,'-')-1) = a.code limit 1)
where exists (select 1 from orig_product_cost b where substr(b.产副品代码,1,instr(b.产副品代码,'-')-1) = a.code and substr(b.产副品代码,instr(b.产副品代码,'-')+1) <> a.name)
    """)
    # 3 统一更新产品对应关系
    exec_command("""
update code_product as a 
   set transfer = (select code from code_product b where b.code=substr(a.code,1,2)||'2'||substr(a.code,4))
 where substr(a.code,3,1)='1'
   and transfer is null    
    """)


# 0618 project_id 改为 project_code
def process_semi_product_in_out():
    logger.info("处理半成品投入产出")
    exec_command("drop table if exists semi_product_in_out")
    exec_command("""
create table semi_product_in_out as
select coalesce(right.project_code,left.project_code) project_code, coalesce(right.mat_group_no,left.mat_group_no) mat_group_no, in_row_no, in_cost_center, in_product_code, in_object, in_wt, in_storage, out_row_no,out_cost_center, out_product_code, out_object, voucher_type, out_seq_no, out_wt 
from 

(
select a.project_code, a.mat_group_no,
       a.row_no out_row_no,
       case when a.first_out='Y' then 'N' else (case when a.cost_center<>' ' and a.account_title_item='01' then 'N' when a.cost_center=' ' and a.account_title_item='01' then 'Y' end) end as in_storage, 
       a.cost_center out_cost_center, a.product_code out_product_code, a.ba_object_sub_1 out_object, 
       case when a.first_out='Y' then '生产成本结转' else (case when a.cost_center<>' ' and a.account_title_item='01' then '入库消耗凭证' when a.cost_center=' ' and a.account_title_item='01' then '入库凭证' end) end voucher_type, 
       a.pass_backlog_seq_no out_seq_no, a.mat_wt out_wt, 
       a.account_title_item out_account_title, a.first_in 
from mat_track_detail a
where a.project_code is not null
  and (a.first_out = 'Y' or a.account_title_item ='01')
order by mat_group_no, pass_backlog_seq_no
) as right 

full outer join

(       
select a.project_code, a.mat_group_no,
       a.row_no in_row_no,
       a.cost_center in_cost_center, a.product_code in_product_code, a.ba_object_sub_1 in_object, 
       a.pass_backlog_seq_no in_seq_no, a.mat_wt in_wt 
from mat_track_detail a
where a.project_code is not null
  and a.account_title_item in ('30','31')
order by mat_group_no, pass_backlog_seq_no
) as left 

on  left.project_code=right.project_code and left.mat_group_no=right.mat_group_no -- and in_seq_no=out_seq_no --??
and in_cost_center=out_cost_center
    """)


# 根据0615讨论结果，0618修改 只有投入无产出的行次标记为：“E 其他”
def process_semi_product_in_out_sum():
    logger.info("半成品投入产出归集")
    exec_command("drop table if exists semi_product_in_out_sum")
    exec_command("""
create table semi_product_in_out_sum as
select *, case when out_product_code in (select code from code_product where name like '%-外销') then 'B 外销类'
               when in_product_code in (select source_code from code_cost_account where source_type = '直接支用-外购半成品') then 'c 外购半成品类'
               when in_product_code = out_product_code then 'D 相同产品'
               when in_product_code is not null and out_product_code is null then 'E 其他' 
               else 'A 普通类' end type
from (
select in_product_code, round(sum(in_wt),4) in_wt, group_concat(in_row_no, ',') in_rows,
       out_product_code, round(sum(out_wt),4) out_wt, group_concat(out_row_no, ',') out_rows
from semi_product_in_out
group by in_product_code, out_product_code
)
    """)


# 0618 project_id 改为 project_code
def process_map_project_product():
    logger.info("aca_ca_首次分配数据源 生成项目产品对应关系")
    exec_command("drop table if exists map_project_product")
    exec_command("""
create table map_project_product as
select --0618 (select 项目编码 from para_project b where b.项目序号=a.project_id) project_code, 
       project_code, cost_center cost_center_code, product_code, ba_object_sub_1, round(sum(mat_wt),4) wt
from mat_track_detail a     
where a.project_code is not null
  and a.account_title_item = '01'
  and a.cost_center <> ' '
group by 1, 2, 3, 4
    """)
    fill_special_product()


# 0602 增加 向项目产品对应关系表中补充特殊产品对应关系
def fill_special_product():
    logger.info("向项目产品对应关系表中补充特殊产品对应关系")
    exec_command("""
insert into map_project_product 
select 项目编码, substr(通过序号1, 1, instr(通过序号1,',')-1), substr(通过序号1, instr(通过序号1,',')+1), null, 重量临界值
  from para_project
 where 牌号信息 is null or 牌号信息 = ''
    """)


# 新增 0618
def get_instorage():
    logger.info("获取当前入库标记信息")
    exec_command("drop table if exists para_instorage_tag")
    exec_command("create table para_instorage_tag as select * from mat_track_detail where project_code is not null")


# 新增 0618
def tag_instorage():
    logger.info("导入并更新入库标记信息")
    exec_command("""
    update mat_track_detail as a
    set in_storage = (select in_storage from para_instorage_tag b where ''||b.row_no=''||a.row_no)
    where ''||row_no in (select ''||row_no from para_instorage_tag)
    """)


def process_aca_db():
    logger.info("aca_db_中间试验品数据源")
    exec_command("drop table if exists aca_db")
    exec_command("""
create table aca_db as
select * from mat_track_detail 
where project_code is not null
  -- 0618 and mat_group_no not in (select mat_group_no from para_inventory_transfer) 
  and coalesce(in_storage,'N') = 'N'
  and account_title_item='01' and cost_center<>' '
  and instr( (select group_concat(out_rows,',') from semi_product_in_out_sum where type='A 普通类'), row_no ) > 0
    """)


def process_aca_dc():
    logger.info("aca_dc_转库存履历码产出明细")
    exec_command("drop table if exists aca_dc")
    exec_command("""
create table aca_dc as
select a.row_no, a.mat_group_no, a.mat_track_no, a.mat_no, a.in_mat_no, pass_backlog_seq_no, whole_backlog_code, whole_backlog_name, 
       d.cost_center_code cost_center, c.transfer product_code, ba_object_sub_1, account_title_item, prod_date, event_related_id, mat_wt, project_code, first_in, first_out  
from mat_track_detail a,
     --0618 para_inventory_transfer b,
     code_product c,
     code_product d
where in_storage = 'Y'       -- 0618 a.mat_group_no = b.mat_group_no
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
