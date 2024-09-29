# -*- coding: UTF-8 -*-

from core.database import *


# 0618 project_id 改为 project_code
def process_ace_ba_bb():
    exec_command("drop table if exists ace_ba_bb")
    exec_command("""
create table ace_ba_bb as 
select a.project_code, (select x.项目名称 from para_project x where x.项目编码=a.project_code limit 1) project_name,
       a.product_code, b.product_name,  
       b.wt_recycle, b.amount_recycle,
       sum(a.mat_wt) mat_wt,  
       sum(a.mat_wt) / (select sum(x.mat_wt) from aca_db x where x.cost_center=a.cost_center and x.product_code=a.product_code) ratio_wt_in_product,
       null project_wt_recycle, null project_amount_recycle
from aca_db a,
     acd_系数设置后指标统计_左 b
where a.cost_center=b.cost_center_code
  and a.product_code=b.product_code
  and b.ratio_recycle is not null 
group by 1, 2, 3, 4, 5, 6
order by 3, 1
    """)
    exec_command("""
update ace_ba_bb as a 
set project_wt_recycle = wt_recycle * ratio_wt_in_product,
    project_amount_recycle = amount_recycle * ratio_wt_in_product
    """)


def process_ace_bc():
    exec_command("drop table if exists ace_bc")
    exec_command("""
create table ace_bc as 
select '中间试验品回收' 类型, product_code 户号代码, product_name 户号名称, 
       project_code 研发编号, (select 项目名称 from para_project x where x.项目编码=project_code limit 1) 研发名称,
       project_wt_recycle 回收重量, ratio_wt_in_product 重量占比, project_amount_recycle 回收金额
from ace_ba_bb
    """)


# 0618 project_id 改为 project_code
def process_ace_ca_cb():
    exec_command("drop table if exists ace_ca_cb")
    exec_command("""
create table ace_ca_cb as 
select a.project_code, (select x.项目名称 from para_project x where x.项目编码=a.project_code limit 1) project_name,
       a.product_code, b.product_name,  
       b.wt_inventory_transfer, b.amount_inventory_transfer, 
       sum(a.mat_wt) mat_wt,  
       sum(a.mat_wt) / (select sum(x.mat_wt) from aca_db x where x.cost_center=a.cost_center and x.product_code=a.product_code) ratio_wt_in_product,
       null project_wt_inventoryrd, null project_amount_inventoryrd
from aca_db a,
     acd_系数设置后指标统计_左 b
where a.cost_center=b.cost_center_code
  and a.product_code=b.product_code
  and b.wt_inventory_transfer is not null 
group by 1, 2, 3, 4, 5, 6
order by 3, 1
    """)
    exec_command("""
update ace_ca_cb as a 
set project_wt_inventoryrd = wt_inventory_transfer * ratio_wt_in_product,
    project_amount_inventoryrd = amount_inventory_transfer * ratio_wt_in_product
    """)


def process_ace_cc():
    exec_command("drop table if exists ace_cc")
    exec_command("""
create table ace_cc as 
select '研发产品入库' 类型, product_code 户号代码, product_name 户号名称, 
       project_code 研发编号, (select 项目名称 from para_project x where x.项目编码=project_code limit 1) 研发名称,
       project_wt_inventoryrd 入库重量, ratio_wt_in_product 重量占比, project_amount_inventoryrd 入库金额
from ace_ca_cb
    """)


# 20240505 去除 code_caccount_transfer，已在优化步骤统一生成code_cost_account
# def process_code_caccount_transfer():
#     exec_command("drop table if exists code_caccount_transfer")
#     exec_command("""
#     create table code_caccount_transfer as
# select distinct 账务代码 code, 账务代码名称 transfer, 备注 trans_type from aca_u_x where 类型='成本科目来源'
#     """)


def process_acf_ad_voucher_instorage_consume():
    exec_command("drop table if exists acf_ad_初始凭证_入库消耗")
    exec_command("""
create table acf_ad_初始凭证_入库消耗 as
select * from voucher_entry 
where 凭证号码 in (select voucher_no from tmp_semi_product_disposal)
    """)


def process_acf_ad_splitted_instorage_consume():
    exec_command("drop table if exists acf_ad_分配后入库消耗")
    exec_command("""
create table acf_ad_分配后入库消耗 as
select * from voucher_splitted 
where 凭证号码 in (select voucher_no from tmp_semi_product_disposal)
    """)


def process_acf_bb_consume_orig():
    exec_command("drop table if exists acf_bb_consume_orig")
    exec_command("""
create table acf_bb_consume_orig as
select * from voucher_entry a
where ( 会计科目中文名称 = '生产成本-基本生产-原料消耗' 
        and 凭证号码 in (select voucher_no from tmp_semi_product_disposal) 
        and 借贷='1' )
    """)
    exec_command("""
insert into acf_bb_consume_orig 
select * from voucher_entry a
where a.凭证号码 in (select voucher_no from tmp_semi_product_disposal) 
  and a.借贷='2'  
  and exists (select 1 from acf_bb_consume_orig b 
                     where a.户号名称=(select coalesce(c.source_name,substr(c.name,instr(c.name,'-')+1)) from code_cost_account c where c.name=b.参号名称 limit 1)  
                       and a.本币金额=b.本币金额 )    
    """)


def process_acf_ba_instorage_orig():
    exec_command("drop table if exists acf_ba_instorage_orig")
    exec_command("""
create table acf_ba_instorage_orig as
select * from voucher_entry a
where 凭证号码 in (select voucher_no from tmp_semi_product_disposal)
  and "index" not in (select "index" from acf_bb_consume_orig)
order by a.参号, 户号, 借贷
    """)


# 0618 新增 入库转消耗接口
def transfer_instorage_to_consume(index):
    if index is None or index < 0:
        raise ValueError("[入库凭证索引号] 参数值 不在合理范围")
    query = "select 1 from acf_ba_instorage_orig where \"index\" = " + str(index)
    row = exec_query(query).fetchone()
    tmp = row[0]
    if tmp is None:
        raise ValueError("[入库凭证索引号] 参数值 在初始入库凭证中不存在")
    exec_command("insert into acf_bb_consume_orig select * from acf_ba_instorage_orig where \"index\" = " + str(index))
    exec_command("delete from acf_ba_instorage_orig where \"index\" = " + str(index))


# 0618 新增 消耗转入库接口
def transfer_consume_to_instorage(index):
    if index is None or index < 0:
        raise ValueError("[凭证索引号] 参数值 不在合理范围")
    query = "select 1 from acf_bb_consume_orig where \"index\" = " + str(index)
    row = exec_query(query).fetchone()
    tmp = row[0]
    if tmp is None:
        raise ValueError("[凭证索引号] 参数值 在初始消耗凭证中不存在")
    exec_command("insert into acf_ba_instorage_orig select * from acf_bb_consume_orig where \"index\" = " + str(index))
    exec_command("delete from acf_bb_consume_orig where \"index\" = " + str(index))


def process_acf_bc_instorage_sum():
    exec_command("drop table if exists acf_bc_instorage_sum")
    exec_command("""
create table acf_bc_instorage_sum as 
select 凭证摘要, 借贷, 会计科目代码, 会计科目中文名称, 户号, 户号名称, 参号, 参号名称, sum(本币金额) 本币金额, sum(数量) 数量
from acf_ba_instorage_orig
group by 1,2,3,4,5,6,7,8
    """)


def process_acf_bd_consume_sum():
    exec_command("drop table if exists acf_bd_consume_sum")
    exec_command("""
create table acf_bd_consume_sum as
select 凭证摘要, 借贷, 会计科目代码, 会计科目中文名称, 户号, 户号名称, 参号, 参号名称, sum(本币金额) 本币金额, sum(数量) 数量
from acf_bb_consume_orig
group by 1,2,3,4,5,6,7,8
    """)


def process_acf_ca_instorage_horizontal():
    exec_command("drop table if exists tmp_acf_ca_instorage_horizontal")
    exec_command("""
create table tmp_acf_ca_instorage_horizontal as 
select rowid groupno, a.会计科目代码, a.会计科目中文名称 借方会计科目, a.户号 产副品户号, a.户号名称 产副品户名, a.本币金额 借方金额, a.数量 借方数量, (select distinct cost_center_code from map_product_account b where a.户号=b.product_code) 借方成本中心代码, (select distinct cost_center_name from map_product_account b where a.户号=b.product_code) 借方成本中心名称, null 贷方金额合计, 
       null 户号, null 户号名称, null 主原料参号, null 主原料参号名称, null 主原料金额, null 主原料对应的产副品代码, null 主原料对应的产副品名称
from acf_bc_instorage_sum a
where a.借贷='1'
  and ( a.会计科目中文名称 like '自制半成品-%' or a.会计科目中文名称 like '委托加工物资-%' )
    """)
    exec_command("""
insert into tmp_acf_ca_instorage_horizontal
select b.groupno, null, null, null, null, null, null, null, null, null,       
       a.户号, a.户号名称, a.参号, a.参号名称, a.本币金额, null, null
from acf_bc_instorage_sum a,
     tmp_acf_ca_instorage_horizontal b
where a.借贷='2'
  and a.会计科目中文名称 = '生产成本-基本生产-结转'
  and a.户号 = b.借方成本中心代码 
  and a.参号名称 like '主原料-%'
    """)
    exec_command("""
update tmp_acf_ca_instorage_horizontal as a
set 贷方金额合计 = (select sum (主原料金额) from tmp_acf_ca_instorage_horizontal b where b.groupno=a.groupno),
    主原料对应的产副品代码 = (select source_code from code_cost_account b where b.name=a.主原料参号名称 and b.source_type='内部移转-自制半成品'),
    主原料对应的产副品名称 = (select source_name from code_cost_account b where b.name=a.主原料参号名称 and b.source_type='内部移转-自制半成品')
    """)
    exec_command("drop table if exists acf_ca_instorage_horizontal")
    exec_command("""
create table acf_ca_instorage_horizontal as 
select * from tmp_acf_ca_instorage_horizontal
order by groupno
    """)


def process_acf_cb_instorage_flow():
    exec_command("drop table if exists acf_cb_instorage_flow")
    exec_command("""
create table acf_cb_instorage_flow as 
select groupno,	产副品户号 产副品代码, 产副品户名 产副品名称, 主原料参号 主原料代码, 主原料参号名称 主原料名称, 主原料对应的产副品代码, 主原料对应的产副品名称, 0 flowno
from acf_ca_instorage_horizontal a 
where a.产副品户名 not like '%-来料加工'
  and exists (select 1 from acf_ca_instorage_horizontal b where b.groupno=a.groupno and b.主原料对应的产副品代码 is not null)
    """)
    exec_command("""
update acf_cb_instorage_flow as a
set 主原料代码 = ( select 主原料参号 from acf_ca_instorage_horizontal b where b.groupno=a.groupno and b.主原料对应的产副品代码 is not null ),
    主原料名称 = ( select 主原料参号名称 from acf_ca_instorage_horizontal b where b.groupno=a.groupno and b.主原料对应的产副品代码 is not null ),
    主原料对应的产副品代码 =  ( select 主原料对应的产副品代码 from acf_ca_instorage_horizontal b where b.groupno=a.groupno and b.主原料对应的产副品代码 is not null ),
    主原料对应的产副品名称 =  ( select 主原料对应的产副品名称 from acf_ca_instorage_horizontal b where b.groupno=a.groupno and b.主原料对应的产副品代码 is not null )
    """)
    exec_command("""
update acf_cb_instorage_flow as a
set flowno = coalesce((select flowno from acf_cb_instorage_flow b where b.产副品代码=a.主原料对应的产副品代码)+1, 0)
    """)


def process_acf_cc_consume_horizontal():
    exec_command("drop table if exists acf_cc_consume_horizontal")
    exec_command("""
create table acf_cc_consume_horizontal as 
select distinct a.flowno, 
            b.会计科目代码 借方会计科目代码, b.会计科目中文名称 借方会计科目, b.户号 借方户号, b.户号名称 借方户号名称, b.参号 借方参号, b.参号名称 借方参号名称, b.本币金额 借方金额, b.数量 借方数量,
            c.会计科目代码 贷方会计科目代码, c.会计科目中文名称 贷方会计科目, c.户号 贷方户号, c.户号名称 贷方户号名称, c.参号 贷方参号, c.参号名称 贷方参号名称, c.本币金额 贷方金额, c.数量 贷方数量  
from (select distinct flowno, 主原料代码, 主原料对应的产副品代码 from acf_cb_instorage_flow) a,
     (select * from acf_bd_consume_sum where 借贷='1') b,
     (select * from acf_bd_consume_sum where 借贷='2') c
where a.主原料代码 = b.参号
  and a.主原料对应的产副品代码 = c.户号
order by flowno
    """)


# 0904 修改 拆分后凭证导入功能增加后，对 统计成本元素 和 acf_da_调整入库凭证 进行口径调整
# 0908 修改 拆分后凭证导入功能增加后，根据沟通情况调整stat_product口径
def process_acf_da_instorage_adjust():
    exec_command("drop table if exists stat_product")
    exec_command("""
create table stat_product as 
select product_code, sum(研发金额*ratio) product_ramount
from stat_caccount a,     
     (
     with o as
       (select cost_center_code, cost_account_code, product_code, sum(month_cost) month_cost
         from map_product_account x 
        where x.month_cost<>0
          and not exists (select 1 from map_ccenter_caccount_svoucher z where z.voucher_type like 'TG-%' and z.cost_center_code=x.cost_center_code and z.cost_account_code=x.cost_account_code)
          and not exists (select 1 from map_ccenter_caccount_svoucher z where z.voucher_type like 'TS-1' and z.cost_center_code=x.cost_center_code and z.cost_account_code=x.cost_account_code)
          and instr(x.product_name, '来料加工') = 0
          and instr(x.product_name, '外销') = 0
        group by 1,2,3)
     select cost_center_code, cost_account_code, product_code, month_cost / ( sum(month_cost) over (partition by o.cost_center_code, o.cost_account_code) ) ratio
       from o 
       ) b
where a.成本中心代码=b.cost_center_code
  and a.成本科目代码=b.cost_account_code
group by 1
    """)
    exec_command("drop table if exists acf_da_instorage_adjust")
    exec_command("""
create table acf_da_instorage_adjust as 
select *, /*0907 修改 将amount_to_adjust默认从null改为为0*/ 0 amount_to_adjust, /*0703 修改 改为本币金额*/ 本币金额 amount_adjusted from acf_bc_instorage_sum
    """)
    exec_command("""
update acf_da_instorage_adjust as a 
   set amount_to_adjust = coalesce((select product_ramount from stat_product b where b.product_code=a.户号),0) ,
       --0630 修改 amount_adjusted 为空时处理
       amount_adjusted = coalesce( ( a.本币金额 - coalesce((select product_ramount from stat_product b where b.product_code=a.户号),0) ), 0 )
 where a.借贷='1'
   and a.会计科目中文名称 like '自制半成品-%'
    """)
    exec_command("""
update acf_da_instorage_adjust as a 
   set amount_to_adjust = coalesce((select 研发金额 from stat_caccount b where b.成本中心代码=a.户号 and b.成本科目代码=a.参号),0),
       --0702 修改 amount_adjusted 为空时处理
       amount_adjusted = coalesce( ( a.本币金额 - coalesce((select 研发金额 from stat_caccount b where b.成本中心代码=a.户号 and b.成本科目代码=a.参号),0) ), 0 )
 where a.借贷='2'
   and a.会计科目中文名称 like '生产成本-基本生产-结转'
    """)
    # 0904 增加以下内容 0906 根据沟通情况修改
    exec_command("drop table if exists stat_salary")
    exec_command("""
create table stat_salary as
select orig.成本科目代码, orig.成本科目名称,  
       coalesce( (select transfer from code_cost_center x where x.code = orig.成本中心代码), orig.成本中心代码 ) 成本中心代码, 
       (select name from code_cost_center where code = coalesce( (select transfer from code_cost_center x where x.code = orig.成本中心代码), orig.成本中心代码 )) 成本中心名称,
       sum(本币金额) 研发金额
from voucher_splitted split,
     (select orig_rowno, 参号 成本科目代码, 参号名称 成本科目名称, 户号 成本中心代码, 户号名称 成本中心名称 from voucher_splitted where orig_caccount is null and orig_vtype is null) orig
where split.会计科目中文名称 like '制造费用-职工薪酬-%'
  and split.orig_rowno = orig.orig_rowno
group by 1,2,3,4
    """)
    exec_command("""
update acf_da_instorage_adjust as a
   set amount_to_adjust = 
   (case when a.参号='5100000' then amount_to_adjust - coalesce((select 研发金额 from stat_salary b where b.成本中心代码=a.户号),0)
         else amount_to_adjust + coalesce((select 研发金额 from stat_salary b where b.成本中心代码=a.户号 and b.成本科目代码=a.参号),0)
    end)
 where 借贷='2'    
    """)
    exec_command("""
update acf_da_instorage_adjust as a
   set amount_adjusted = coalesce( (a.本币金额 - a.amount_to_adjust),0 )
 where 借贷='2'    
    """)


def process_acf_db_consume_adjust():
    exec_command("drop table if exists acf_db_consume_adjust")
    exec_command("""
create table acf_db_consume_adjust as 
select *, null amount_to_adjust, 本币金额 amount_adjusted from acf_bd_consume_sum
    """)
