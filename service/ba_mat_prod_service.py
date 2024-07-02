# -*- coding: UTF-8 -*-

from core.database import *


def process_acg_ba_semi_voucher():
    exec_command("drop table if exists acg_ba_待调整序时账")
    exec_command("""
create table acg_ba_待调整序时账 as 
select * from orig_voucher_entry a
where 凭证号码 in ( select 凭证号码 from orig_voucher_entry where "index" in (select max("index") from orig_voucher_entry where instr(凭证摘要,'半成品抛帐')>0 union all select max("index") from orig_voucher_entry where instr(凭证摘要,'产成品主营业务成本抛帐')>0 ) )

    """)
    exec_command("""
    update ace_ba_bb as a 
set project_wt_recycle = wt_recycle * ratio_wt_in_product,
    project_amount_recycle = amount_recycle * ratio_wt_in_product
    """)


def process_acg_da_rd_unit_price():
    exec_command("drop table if exists tmp_acg_de_入库单价重算")
    exec_command("""
create table tmp_acg_de_入库单价重算 as 
select a.groupno, null flowno,
       a.会计科目代码 借方会计科目代码, a.借方会计科目, b.户号, b.户号名称, b.amount_adjusted 借方原金额, b.数量 借方原重量, 
       null 贷方会计科目代码, null 贷方会计科目, null 贷方户号, null 贷方户号名称,  null 贷方参号, null 贷方参号名称, null 贷方金额
from acf_ca_instorage_horizontal a,
     acf_da_instorage_adjust b
where a.产副品户名 not like '%-来料加工'
  and b.借贷='1'
  and a.会计科目代码 = b.会计科目代码
  and a.产副品户号 = b.户号
    """)
    exec_command("""
insert into tmp_acg_de_入库单价重算
select a.groupno, null, null, null, null, null, null, null, b.会计科目代码, b.会计科目中文名称, b.户号, b.户号名称, b.参号, b.参号名称, b.amount_adjusted
from acf_ca_instorage_horizontal a,
     acf_da_instorage_adjust b
where b.借贷='2'
  and a.户号 = b.户号
  and a.主原料参号 = b.参号
  and a.groupno in (select groupno from tmp_acg_de_入库单价重算)    
    """)
    exec_command("""
update tmp_acg_de_入库单价重算 as a
   set flowno = 0    
    """)
    exec_command("""
drop table if exists acg_de_入库单价重算    
    """)
    exec_command("""
create table acg_de_入库单价重算 as 
select *, null 半成品产出重量, null 调整后生产成本金额, null 调整后生产成本重量, null 调整后入库金额, null 调整后入库重量 
from tmp_acg_de_入库单价重算 order by groupno    
    """)
    exec_command("""
drop table if exists acg_da_研发单价重算    
    """)
    exec_command("""
create table acg_da_研发单价重算 as
select b.flowno, b.借方会计科目代码 会计科目代码, b.借方会计科目 会计科目中文名称, a.out_product_code 户号, (select name from code_product x where x.code=a.out_product_code) 户号名称, b.借方原金额 原金额, b.借方原重量 原重量,
       null 中间试验品回收重量, null 中间试验品回收金额, null 研发产品入库重量, null 研发产品入库金额, null 重量合计, null 金额合计
from semi_product_in_out_sum a,
     acg_de_入库单价重算 b
where a.type = 'A 普通类'
  and a.out_product_code is not null
  and a.out_product_code=b.户号    
    """)
    exec_command("""
update acg_da_研发单价重算 as a
   set 中间试验品回收重量 = (select sum(回收重量) from ace_bc where 户号代码=a.户号),
       中间试验品回收金额 = (select sum(回收金额) from ace_bc where 户号代码=a.户号)    
    """)
    exec_command("""
update acg_da_研发单价重算 as a
   set 研发产品入库重量 = (select sum(入库重量) from ace_cc where 户号代码=a.户号),
       研发产品入库金额 = (select sum(入库金额) from ace_cc where 户号代码=a.户号)    
    """)
    exec_command("""
update acg_da_研发单价重算 as a 
   set 重量合计 = coalesce(中间试验品回收重量,0) + coalesce(研发产品入库重量,0),
       金额合计 = coalesce(中间试验品回收金额,0) + coalesce(研发产品入库金额,0)    
    """)


def process_acg_db_rd_amount():
    exec_command("drop table if exists acg_db_研发金额重算")
    exec_command("""
create table acg_db_研发金额重算 as 
select a.类型, b.会计科目代码, b.会计科目中文名称, a.户号代码, a.户号名称, a.研发编号, a.研发名称, a.回收重量 研发重量, a.重量占比, a.回收金额 研发金额 
from ace_bc a, --excel acg 中名称为AE_中间试验品回收明细
     acg_da_研发单价重算 b 
where a.户号代码=b.户号    
    """)
    exec_command("""
insert into acg_db_研发金额重算
select a.类型, b.会计科目代码, b.会计科目中文名称, a.户号代码, a.户号名称, a.研发编号, a.研发名称, a.入库重量, a.重量占比, a.入库金额 
from ace_cc a, --excel acg 中名称为AE_研发产品入库明细
     acg_da_研发单价重算 b 
where a.户号代码=b.户号    
    """)


def process_recalculate():
    # 1 初始化
    exec_command("drop table if exists acg_dd_消耗单价重算")
    exec_command("""
create table acg_dd_消耗单价重算 as 
select a.flowno, c.会计科目代码 贷方会计科目代码, c.会计科目中文名称 贷方会计科目名称, c.户号 贷方户号, c.户号名称 贷方户号名称, c.amount_adjusted 贷方原金额, c.数量 贷方原重量,
       b.会计科目代码 借方会计科目代码, b.会计科目中文名称 借方会计科目名称, b.户号 借方户号, b.户号名称 借方户号名称, b.参号 借方参号, b.参号名称 借方参号名称, b.amount_adjusted 借方原金额, b.数量 借方原重量,
       null 调整后单价, null 调整后自制半成品贷方金额, null 调整后研发金额, null 研发占用量（投入）, null 调整后生产成本金额, null 调整后生产成本重量
from (select distinct flowno, 贷方会计科目代码, 贷方户号, 借方会计科目代码, 借方户号, 借方参号 from acf_cc_consume_horizontal) a, 
     (select * from acf_db_consume_adjust where 借贷='1') b,
     (select * from acf_db_consume_adjust where 借贷='2') c
where a.贷方会计科目代码 = c.会计科目代码
  and a.贷方户号 = c.户号
  and a.借方会计科目代码 = b.会计科目代码
  and a.借方户号 = b.户号
  and a.借方参号 = b.参号
    """)
    exec_command("""
drop table if exists acg_dc_半成品生产收发存表
    """)
    exec_command("""
create table acg_dc_半成品生产收发存表 as 
select row_number() over(order by 会计科目代码, 产副品代码) rowno, *, null 数量, null 金额, null 单价 from 
(
select flowno, 借方会计科目代码 会计科目代码, 借方会计科目 会计科目名称, 户号 产副品代码, 户号名称 产副品名称 
from acg_de_入库单价重算
where 借方会计科目代码 is not null
union 
select flowno, 贷方会计科目代码 会计科目代码, 贷方会计科目名称 会计科目名称, 贷方户号 产副品代码, 贷方户号名称 产副品名称 
from acg_dd_消耗单价重算
) a,
(select '1期初信息' 类别 union select '2生产入库' union select '3研发入库' union select '4本期结存' union select '5本期出库' union select '6期末信息') b
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a
set 数量=(select 期末数量 from inventory_summary b where b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码),
    金额=(select 期末金额 from inventory_summary b where b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码),
    单价=(select 期末单价 from inventory_summary b where b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码)
where 类别='1期初信息'   
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a
set 数量=(select 重量合计 from acg_da_研发单价重算 b where b.会计科目代码=a.会计科目代码 and b.户号=a.产副品代码),
    金额=(select 金额合计 from acg_da_研发单价重算 b where b.会计科目代码=a.会计科目代码 and b.户号=a.产副品代码)
where 类别='3研发入库'    
    """)
    exec_command("""
update acg_de_入库单价重算 as a 
   set 半成品产出重量 = (select 重量合计 from acg_da_研发单价重算 b where b.户号=a.户号)    
    """)
    # 2.0 单价重算准备
    exec_command("""
drop table if exists tmp_current_link_in_flow    
    """)
    exec_command("""
create table tmp_current_link_in_flow as 
select flowno, groupno, group_concat(借方会计科目代码) 借方会计科目代码, group_concat(户号) 借方户号, group_concat(贷方户号) 贷方户号, group_concat(贷方参号) 贷方参号
from acg_de_入库单价重算 a
where not exists (select 1 from acg_dd_消耗单价重算 b, acg_de_入库单价重算 c 
                    where c.groupno=a.groupno 
                      and b.借方户号=c.贷方户号 and b.借方参号=c.贷方参号)
group by flowno, groupno    
    """)
    # 2.1 单价重算第1步
    exec_command("""
update acg_de_入库单价重算 as a 
   set 调整后生产成本金额 = 贷方金额
 where groupno in (select groupno from tmp_current_link_in_flow)    
    """)
    exec_command("""
update acg_de_入库单价重算 as a 
   set 调整后入库金额 = 借方原金额 - (select sum(贷方金额) from acg_de_入库单价重算 b where b.groupno=a.groupno) + (select coalesce(sum(调整后生产成本金额),0) from acg_de_入库单价重算 b where b.groupno=a.groupno),
       调整后入库重量 = 借方原重量 - coalesce(半成品产出重量,0)
 where 借方原金额 is not null
   and groupno in (select groupno from tmp_current_link_in_flow)    
    """)
    # 2.1 单价重算第2步
    exec_command("""
update acg_dc_半成品生产收发存表 as a
set 金额=(select 调整后入库金额 from acg_de_入库单价重算 b where b.借方会计科目代码=a.会计科目代码 and b.户号=a.产副品代码),
    数量=(select 调整后入库重量 from acg_de_入库单价重算 b where b.借方会计科目代码=a.会计科目代码 and b.户号=a.产副品代码)    
where 类别='2生产入库'
  and exists (select 1 from tmp_current_link_in_flow b 
               where b.借方会计科目代码=a.会计科目代码
                 and b.借方户号=a.产副品代码)    
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a
set 金额=(select sum(金额) from acg_dc_半成品生产收发存表 b where b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码 and b.类别 in ('1期初信息','2生产入库','3研发入库')),
    数量=(select sum(数量) from acg_dc_半成品生产收发存表 b where b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码 and b.类别 in ('1期初信息','2生产入库','3研发入库'))    
where 类别='4本期结存'
  and exists (select 1 from tmp_current_link_in_flow b 
               where b.借方会计科目代码=a.会计科目代码
                 and b.借方户号=a.产副品代码)    
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a
set 单价 = 金额 / 数量    
where 类别='4本期结存'
  and exists (select 1 from tmp_current_link_in_flow b 
               where b.借方会计科目代码=a.会计科目代码
                 and b.借方户号=a.产副品代码)    
    """)
    # 2.3 单价重算第3步
    exec_command("""
update acg_dd_消耗单价重算 as a
   set 调整后单价 = (select 单价 from acg_dc_半成品生产收发存表 b where b.类别='4本期结存' and b.会计科目代码=a.贷方会计科目代码 and b.产副品代码=a.贷方户号)
where exists (select 1 from tmp_current_link_in_flow b 
               where b.借方会计科目代码=a.贷方会计科目代码
                 and b.借方户号=a.贷方户号)    
    """)
    exec_command("""
update acg_dd_消耗单价重算 as a
   set 调整后自制半成品贷方金额 =  调整后单价 * 贷方原重量,
       调整后生产成本金额 = 调整后单价 * 借方原重量,
       调整后生产成本重量 = 借方原重量,
       调整后研发金额 = 调整后单价 * 研发占用量（投入）
where exists (select 1 from tmp_current_link_in_flow b 
               where b.借方会计科目代码=a.贷方会计科目代码
                 and b.借方户号=a.贷方户号)    
    """)
    # 单价重算第4步
    exec_command("""
update acg_de_入库单价重算 as a 
   set 调整后生产成本金额 = (select 调整后生产成本金额 from acg_dd_消耗单价重算 b where b.借方户号=a.贷方户号 and b.借方参号=a.贷方参号),
       调整后生产成本重量 = (select 调整后生产成本重量 from acg_dd_消耗单价重算 b where b.借方户号=a.贷方户号 and b.借方参号=a.贷方参号)
 where exists (select 1 from acg_dd_消耗单价重算 b where b.借方户号=a.贷方户号 and b.借方参号=a.贷方参号)    
    """)
    exec_command("""
update acg_de_入库单价重算 as a 
   set 调整后生产成本金额 = 贷方金额
 where not exists (select 1 from acg_dd_消耗单价重算 b where b.借方户号=a.贷方户号 and b.借方参号=a.贷方参号)
   and groupno not in (select groupno from tmp_current_link_in_flow)    
    """)
    exec_command("""
update acg_de_入库单价重算 as a 
   set 调整后入库金额 = 借方原金额 - (select sum(贷方金额) from acg_de_入库单价重算 b where b.groupno=a.groupno) + (select coalesce(sum(调整后生产成本金额),0) from acg_de_入库单价重算 b where b.groupno=a.groupno),
       调整后入库重量 = 借方原重量 - coalesce(半成品产出重量,0)
 where 借方原金额 is not null
   and groupno not in (select groupno from tmp_current_link_in_flow)    
    """)
    # 2.5 单价重算第5步
    exec_command("""
update acg_dc_半成品生产收发存表 as a
set 金额=(select 调整后入库金额 from acg_de_入库单价重算 b where b.借方会计科目代码=a.会计科目代码 and b.户号=a.产副品代码),
    数量=(select 调整后入库重量 from acg_de_入库单价重算 b where b.借方会计科目代码=a.会计科目代码 and b.户号=a.产副品代码)    
where 类别='2生产入库'
  and not exists (select 1 from tmp_current_link_in_flow b 
               where b.借方会计科目代码=a.会计科目代码
                 and b.借方户号=a.产副品代码)    
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a
set 金额=(select sum(金额) from acg_dc_半成品生产收发存表 b where b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码 and b.类别 in ('1期初信息','2生产入库','3研发入库')),
    数量=(select sum(数量) from acg_dc_半成品生产收发存表 b where b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码 and b.类别 in ('1期初信息','2生产入库','3研发入库'))    
where 类别='4本期结存'
  and not exists (select 1 from tmp_current_link_in_flow b 
               where b.借方会计科目代码=a.会计科目代码
                 and b.借方户号=a.产副品代码)    
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a
set 单价 = 金额 / 数量    
where 类别='4本期结存'
  and not exists (select 1 from tmp_current_link_in_flow b 
               where b.借方会计科目代码=a.会计科目代码
                 and b.借方户号=a.产副品代码)    
    """)
    # 2.6 单价重算第6步
    exec_command("drop table if exists acg_fa_调整入库凭证")
    exec_command("""
create table acg_fa_调整入库凭证 as 
select *, null p重算单价调整金额, null q重算单价调整重量, null r转库存计价调整金额, null s最终金额, null t最终重量
from acf_da_instorage_adjust
    """)
    exec_command("""
drop table if exists acg_df_自制半成品转库存计价        
    """)
    exec_command("""
create table acg_df_自制半成品转库存计价 as 
select a.会计科目代码 贷方会计科目代码, a.会计科目中文名称 b贷方会计科目, a.户号 贷方户号, a.户号名称 c贷方户号名称, a.本币金额 d贷方金额, a.数量 e贷方重量, 
       b.会计科目代码 借方会计科目代码, b.会计科目中文名称 f借方会计科目, b.户号 借方户号, b.户号名称 g借方户号名称, b.本币金额 h借方金额, b.数量 i借方重量, 
       c.单价 j新单价, b.数量* c.单价 k新结转成本
from
(
select 会计科目代码, 会计科目中文名称, 户号, 户号名称, 本币金额, 数量 from acg_fa_调整入库凭证 a 
where a.借贷='2'
  and a.会计科目中文名称 like '自制半成品-%'
  and a.户号名称 not like '%-来料加工') a,
(
select 会计科目代码, 会计科目中文名称, 户号, 户号名称, 本币金额, 数量 from acg_fa_调整入库凭证 a 
where a.借贷='1'
  and a.会计科目中文名称 like '库存商品-%') b,
acg_dc_半成品生产收发存表 c
where a.户号名称||'-外销' = b.户号名称
  and a.会计科目代码 = c.会计科目代码
  and a.户号=c.产副品代码
  and c.类别='4本期结存'    
    """)
    exec_command("""
drop table if exists acg_dg_自制半成品销售计价    
    """)
    exec_command("""
create table acg_dg_自制半成品销售计价 as 
select a.会计科目代码 贷方会计科目代码, a.会计科目中文名称 b贷方会计科目, a.户号 贷方户号, a.户号名称 c贷方户号名称, a.本币金额 d贷方金额, a.数量 e贷方重量, a."index" 贷方行号,
       b.会计科目代码 借方会计科目代码, b.会计科目中文名称 f借方会计科目, b.户号 借方户号, b.户号名称 g借方户号名称, b.本币金额 h借方金额, b.数量 i借方重量, b."index" 借方行号,
       c.单价 j新单价, b.数量* c.单价 k新结转成本
from
(
select "index", 会计科目代码, 会计科目中文名称, 户号, 户号名称, 本币金额, 数量 from acg_ba_待调整序时账 a 
where a.借贷='2'
  and a.会计科目中文名称 like '自制半成品-%') a,
(
select "index", 会计科目代码, 会计科目中文名称, 户号, 户号名称, 本币金额, 数量 from acg_ba_待调整序时账 a 
where a.借贷='1'
  and a.会计科目中文名称 like '主营业务成本-钢铁产品') b,
acg_dc_半成品生产收发存表 c
where a.户号 = b.户号
  and a.会计科目代码 = c.会计科目代码
  and a.户号=c.产副品代码
  and c.类别='4本期结存'    
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a
set 数量=(select coalesce(研发占用量（投入）,0) + 调整后生产成本重量 from acg_dd_消耗单价重算 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.产副品代码),
    --0618 修改 修复BUG CP-0002 未汇总问题
    金额=(select sum(coalesce(调整后研发金额,0) + 调整后生产成本金额) from acg_dd_消耗单价重算 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.产副品代码)
where 类别='5本期出库'
  and exists (select 1 from tmp_current_link_in_flow b 
               where b.借方会计科目代码=a.会计科目代码 
                 and b.借方户号=a.产副品代码)    
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a 
   set 金额 = (select k新结转成本 from acg_df_自制半成品转库存计价 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.产副品代码),
       数量 = (select i借方重量 from acg_df_自制半成品转库存计价 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.产副品代码)
 where 类别 = '5本期出库'
   and exists (select 1 from acg_df_自制半成品转库存计价 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.产副品代码)    
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a 
   set 金额 = (select k新结转成本 from acg_dg_自制半成品销售计价 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.产副品代码),
       数量 = (select i借方重量 from acg_dg_自制半成品销售计价 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.产副品代码)
 where 类别 = '5本期出库'
   and exists (select 1 from acg_dg_自制半成品销售计价 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.产副品代码)    
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a 
   set 金额 = (select 金额 from acg_dc_半成品生产收发存表 b where b.类别='4本期结存' and b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码) - (select 金额 from acg_dc_半成品生产收发存表 b where b.类别='5本期出库' and b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码),
       数量 = (select 数量 from acg_dc_半成品生产收发存表 b where b.类别='4本期结存' and b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码) - (select 数量 from acg_dc_半成品生产收发存表 b where b.类别='5本期出库' and b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码)
 where 类别 = '6期末信息'    
    """)
    exec_command("""
update acg_dc_半成品生产收发存表 as a 
   set 单价 = 金额 / 数量    
 where 类别 = '6期末信息'    
    """)


def process_merchandise_inventory():
    exec_command("""
drop table if exists acg_ea_当期库存商品收发存表    
    """)
    exec_command("""
create table acg_ea_当期库存商品收发存表 as 
select row_number() over(order by 会计科目代码, 产副品代码) rowno, *, null 数量, null 金额, null 单价 from 
(
select rowid groupno, 借方会计科目代码 会计科目代码, f借方会计科目 会计科目名称, 借方户号 产副品代码, g借方户号名称 产副品名称 
from acg_df_自制半成品转库存计价
) a,
(select '1期初信息' 类别 union select '2本期入库' union select '3本期结存' union select '4本期出库' union select '5期末信息') b    
    """)
    exec_command("""
update acg_ea_当期库存商品收发存表 as a
set 数量=(select 期末数量 from inventory_summary b where b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码),
    金额=(select 期末金额 from inventory_summary b where b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码),
    单价=(select 期末单价 from inventory_summary b where b.会计科目代码=a.会计科目代码 and b.产副品代码=a.产副品代码)
where 类别='1期初信息'    
    """)
    exec_command("""
update acg_ea_当期库存商品收发存表 as a
set 数量=(select i借方重量 from acg_df_自制半成品转库存计价 b where b.借方会计科目代码=a.会计科目代码 and b.借方户号=a.产副品代码),
    金额=(select k新结转成本 from acg_df_自制半成品转库存计价 b where b.借方会计科目代码=a.会计科目代码 and b.借方户号=a.产副品代码)
where 类别='2本期入库'    
    """)
    exec_command("""
update acg_ea_当期库存商品收发存表 as a
set 数量=(select sum(数量) from acg_ea_当期库存商品收发存表 b where b.groupno=a.groupno and b.类别 in ('1期初信息', '2本期入库')),
    金额=(select sum(金额) from acg_ea_当期库存商品收发存表 b where b.groupno=a.groupno and b.类别 in ('1期初信息', '2本期入库')),
    单价=(select sum(金额) from acg_ea_当期库存商品收发存表 b where b.groupno=a.groupno and b.类别 in ('1期初信息', '2本期入库')) / (select sum(数量) from acg_ea_当期库存商品收发存表 b where b.groupno=a.groupno and b.类别 in ('1期初信息', '2本期入库'))
where 类别='3本期结存'    
    """)
    exec_command("""
drop table if exists acg_eb_库存商品销售计价    
    """)
    exec_command("""
create table acg_eb_库存商品销售计价 as 
select row_number() over() rowno, 
       a.会计科目代码 贷方会计科目代码, a.会计科目中文名称 b贷方会计科目, a.户号 贷方户号, a.户号名称 c贷方户号名称, a.本币金额 d贷方金额, a.数量 e贷方重量, a."index" "贷方行号",
       b.会计科目代码 借方会计科目代码, b.会计科目中文名称 f借方会计科目, b.户号 借方户号, b.户号名称 g借方户号名称, b.本币金额 h借方金额, b.数量 i借方重量, b."index" "借方行号",
       c.单价 j新单价, b.数量* c.单价 k新结转成本
from
(
select "index", 会计科目代码, 会计科目中文名称, 户号, 户号名称, 本币金额, 数量 from acg_ba_待调整序时账 where 借贷='2' and 会计科目中文名称 like '库存商品-%') a,
(
select "index", 会计科目代码, 会计科目中文名称, 户号, 户号名称, 本币金额, 数量 from acg_ba_待调整序时账 where 借贷='1' and 会计科目中文名称 like '主营业务成本-钢铁产品') b,
acg_ea_当期库存商品收发存表 c
where a.户号 = b.户号
  and a.本币金额 = b.本币金额
  and a.会计科目代码 = c.会计科目代码
  and a.户号=c.产副品代码
  and c.类别='3本期结存'    
    """)
    exec_command("""
update acg_ea_当期库存商品收发存表 as a
set 数量=(select i借方重量 from acg_eb_库存商品销售计价 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.产副品代码),
    金额=(select k新结转成本 from acg_eb_库存商品销售计价 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.产副品代码)
where 类别='4本期出库'    
    """)
    exec_command("""
update acg_ea_当期库存商品收发存表 as a
set 数量=(select 数量 from acg_ea_当期库存商品收发存表 b where b.groupno=a.groupno and b.类别='3本期结存') - (select coalesce(数量,0) from acg_ea_当期库存商品收发存表 b where b.groupno=a.groupno and b.类别='4本期出库'),
    金额=(select 金额 from acg_ea_当期库存商品收发存表 b where b.groupno=a.groupno and b.类别='3本期结存') - (select coalesce(金额,0) from acg_ea_当期库存商品收发存表 b where b.groupno=a.groupno and b.类别='4本期出库')
where 类别='5期末信息'    
    """)
    exec_command("""
update acg_ea_当期库存商品收发存表 as a
set 单价 = 金额 / 数量
where 类别='5期末信息'    
    """)


def process_acg_fa_adjust_instorage_voucher():
    exec_command("""
update acg_fa_调整入库凭证 as a 
   set p重算单价调整金额 = (select 调整后入库金额 from acg_de_入库单价重算 b where b.借方会计科目代码=a.会计科目代码 and b.户号=a.户号) - amount_adjusted,
       q重算单价调整重量 = (select 调整后入库重量 from acg_de_入库单价重算 b where b.借方会计科目代码=a.会计科目代码 and b.户号=a.户号) - 数量
where 借贷='1'  
  and exists (select 1 from acg_de_入库单价重算 b where b.借方会计科目代码=a.会计科目代码 and b.户号=a.户号)    
    """)
    exec_command("""
update acg_fa_调整入库凭证 as a 
   set p重算单价调整金额 = (select sum(coalesce(调整后生产成本金额,0)) - sum(coalesce(贷方金额,0)) from acg_de_入库单价重算 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.户号 and b.贷方参号=a.参号),
       q重算单价调整重量 = 0
 where 借贷='2'  
  and exists (select 1 from acg_de_入库单价重算 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.户号 and b.贷方参号=a.参号)    
    """)
    exec_command("""
update acg_fa_调整入库凭证 as a 
   set r转库存计价调整金额 = (select k新结转成本-d贷方金额 from acg_df_自制半成品转库存计价 b where b.借方会计科目代码=a.会计科目代码 and b.借方户号=a.户号)
 where 借贷='1'  
   and exists (select 1 from acg_df_自制半成品转库存计价 b where b.借方会计科目代码=a.会计科目代码 and b.借方户号=a.户号)    
    """)
    exec_command("""
update acg_fa_调整入库凭证 as a 
   set r转库存计价调整金额 = (select k新结转成本-d贷方金额 from acg_df_自制半成品转库存计价 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.户号)
 where 借贷='2'  
  and exists (select 1 from acg_df_自制半成品转库存计价 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.户号)    
    """)
    exec_command("""
update acg_fa_调整入库凭证 as a 
   --0630 修改 amount_adjusted 为空时处理
   set s最终金额 = coalesce(amount_adjusted,0) + coalesce(p重算单价调整金额,0) + coalesce(r转库存计价调整金额,0),
       t最终重量 = coalesce(数量,0) + coalesce(q重算单价调整重量,0)    
    """)


def process_acg_fb_adjust_consume_voucher():
    exec_command("""
drop table if exists acg_fb_调整消耗凭证    
    """)
    exec_command("""
create table acg_fb_调整消耗凭证 as 
select *, null p重算单价调整金额, null q重算单价调整重量, null r最终金额, null s最终重量
from acf_db_consume_adjust    
    """)
    exec_command("""
update acg_fb_调整消耗凭证 as a 
   set p重算单价调整金额 = (select 调整后生产成本金额 from acg_dd_消耗单价重算 b where b.借方会计科目代码=a.会计科目代码 and b.借方户号=a.户号 and b.借方参号=a.参号) - a.amount_adjusted,
       q重算单价调整重量 = (select 调整后生产成本重量 from acg_dd_消耗单价重算 b where b.借方会计科目代码=a.会计科目代码 and b.借方户号=a.户号 and b.借方参号=a.参号) - a.数量
 where 借贷='1'  
  and exists (select 1 from acg_dd_消耗单价重算 b where b.借方会计科目代码=a.会计科目代码 and b.借方户号=a.户号 and b.借方参号=a.参号)    
    """)
    exec_command("""
update acg_fb_调整消耗凭证 as a 
   set p重算单价调整金额 = (select max(coalesce(调整后自制半成品贷方金额,0)) from acg_dd_消耗单价重算 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.户号) - amount_adjusted,
       q重算单价调整重量 = 0
 where 借贷='2'  
  and exists (select 1 from acg_dd_消耗单价重算 b where b.贷方会计科目代码=a.会计科目代码 and b.贷方户号=a.户号)    
    """)
    exec_command("""
update acg_fb_调整消耗凭证 as a 
   set r最终金额 = coalesce(amount_adjusted,0) + coalesce(p重算单价调整金额,0),
       s最终重量 = coalesce(数量,0) + coalesce(q重算单价调整重量,0)    
    """)


def process_acg_fc_append_instorage_voucher():
    exec_command("""
drop table if exists acg_fc_入库凭证追加行    
    """)
    exec_command("""
create table acg_fc_入库凭证追加行 as 
select '中间试验品回收' a凭证摘要, '1' b借贷, 会计科目代码 c会计科目代码, 会计科目中文名称 d会计科目中文名称, 户号代码 e户号, 户号名称 f户号名称, null g参号, null h参号名称, sum(研发金额) i本币金额, sum(研发重量) j数量
from acg_db_研发金额重算 a 
where a.类型='中间试验品回收'
group by 3,4,5,6    
    """)
    exec_command("""
insert into acg_fc_入库凭证追加行
select '中间试验品回收' a凭证摘要, '2' b借贷, b.研发科目编码 c会计科目代码, b.研发科目名称 d会计科目中文名称, b.项目编号 e户号, b.项目名称 f户号名称, null g参号, null h参号名称, sum(a.研发金额*b.比例) i本币金额, null j数量
from acg_db_研发金额重算 a,
     stat_project b
where a.类型='中间试验品回收'
  and a.研发编号=b.项目编号
group by 5,6,3,4    
    """)
    exec_command("""
insert into acg_fc_入库凭证追加行
select '研发产品入库' a凭证摘要, '1' b借贷, 会计科目代码 c会计科目代码, 会计科目中文名称 d会计科目中文名称, 户号代码 e户号, 户号名称 f户号名称, null g参号, null h参号名称, sum(研发金额) i本币金额, sum(研发重量) j数量
from acg_db_研发金额重算 a 
where a.类型='研发产品入库'
group by 3,4,5,6    
    """)
    exec_command("""
insert into acg_fc_入库凭证追加行
select '研发产品入库' a凭证摘要, '2' b借贷, b.研发科目编码 c会计科目代码, b.研发科目名称 d会计科目中文名称, b.项目编号 e户号, b.项目名称 f户号名称, null g参号, null h参号名称, sum(a.研发金额*b.比例) i本币金额, null j数量
from acg_db_研发金额重算 a,
     stat_project b
where a.类型='研发产品入库'
  and a.研发编号=b.项目编号
group by 5,6,3,4    
    """)


def process_acg_fd_main_cost_voucher():
    exec_command("""
drop table if exists acg_fd_调后转主营成本凭证    
    """)
    exec_command("""
create table acg_fd_调后转主营成本凭证 as 
select "index" 位置, case when 凭证摘要 like '半成品%' then '半成品抛帐' when 凭证摘要 like '产成品%' then '产成品抛账' end 类型, null b原金额, null c原重量,
       会计期, 凭证日期, 凭证号码, 凭证摘要, 借贷, 会计科目代码, 会计科目中文名称, 户号, 户号名称, 参号, 参号名称, 附加类别一, 附加类别二, 币种, 本币金额, 外币金额, 数量
from acg_ba_待调整序时账    
    """)
    exec_command("""
update acg_fd_调后转主营成本凭证 as a 
   set b原金额 = 本币金额,
       c原重量 = 数量,
       本币金额 = (select k新结转成本 from acg_dg_自制半成品销售计价 b where b.贷方行号=a.位置 or b.借方行号=a.位置)
where 类型 = '半成品抛帐'
  and exists (select 1 from acg_dg_自制半成品销售计价 b where b.贷方行号=a.位置 or b.借方行号=a.位置)    
    """)
    exec_command("""
update acg_fd_调后转主营成本凭证 as a 
   set b原金额 = 本币金额,
       c原重量 = 数量,
       本币金额 = (select k新结转成本 from acg_eb_库存商品销售计价 b where b.贷方行号=a.位置 or b.借方行号=a.位置)
where 类型 = '产成品抛账'
  and exists (select 1 from acg_eb_库存商品销售计价 b where b.贷方行号=a.位置 or b.借方行号=a.位置)    
    """)


def process_acg_fe_inventory_summary():
    exec_command("""
drop table if exists acg_fe_当期收发存结果表    
    """)
    exec_command("""
create table acg_fe_当期收发存结果表 as 
select 'B-'||sum(case when 类别='1期初信息' then rowno else 0 end) 编码, 会计科目代码, 会计科目名称,  产副品代码, 产副品名称,
       sum(case when 类别='1期初信息' then 数量 else 0 end) 期初数量, 
       sum(case when 类别='1期初信息' then 金额 else 0 end) 期初金额,
       sum(case when 类别='1期初信息' then 单价 else 0 end) 期初单价,
       sum(case when 类别='2生产入库' then 数量 else 0 end) 生产入库数量,
       sum(case when 类别='2生产入库' then 金额 else 0 end) 生产入库金额, 
       sum(case when 类别='3研发入库' then 数量 else 0 end) 研发入库数量,
       sum(case when 类别='3研发入库' then 金额 else 0 end) 研发入库金额,
       sum(case when 类别='4本期结存' then 数量 else 0 end) 结存数量, 
       sum(case when 类别='4本期结存' then 金额 else 0 end) 结存金额,
       sum(case when 类别='4本期结存' then 单价 else 0 end) 结存单价,
       sum(case when 类别='5本期出库' then 数量 else 0 end) 出库数量,
       sum(case when 类别='5本期出库' then 金额 else 0 end) 出库金额,       
       sum(case when 类别='6期末信息' then 数量 else 0 end) 期末数量,
       sum(case when 类别='6期末信息' then 金额 else 0 end) 期末金额, 
       sum(case when 类别='6期末信息' then 单价 else 0 end) 期末单价
from acg_dc_半成品生产收发存表
group by 2,3,4,5
union all
select 'C-'||sum(case when 类别='1期初信息' then rowno else 0 end) 编码, 会计科目代码, 会计科目名称,  产副品代码, 产副品名称,
       sum(case when 类别='1期初信息' then 数量 else 0 end) 期初数量, 
       sum(case when 类别='1期初信息' then 金额 else 0 end) 期初金额,
       sum(case when 类别='1期初信息' then 单价 else 0 end) 期初单价,
       sum(case when 类别='2本期入库' then 数量 else 0 end) 生产入库数量,
       sum(case when 类别='2本期入库' then 金额 else 0 end) 生产入库金额, 
       null, null,
       sum(case when 类别='3本期结存' then 数量 else 0 end) 结存数量, 
       sum(case when 类别='3本期结存' then 金额 else 0 end) 结存金额,
       sum(case when 类别='3本期结存' then 单价 else 0 end) 结存单价,
       sum(case when 类别='4本期出库' then 数量 else 0 end) 出库数量,
       sum(case when 类别='4本期出库' then 金额 else 0 end) 出库金额,       
       sum(case when 类别='5期末信息' then 数量 else 0 end) 期末数量,
       sum(case when 类别='5期末信息' then 金额 else 0 end) 期末金额, 
       sum(case when 类别='5期末信息' then 单价 else 0 end) 期末单价
from acg_ea_当期库存商品收发存表
group by 2,3,4,5    
    """)


def process_acg_ad_stat_project():
    exec_command("""
drop table if exists acg_ad_统计_研发项目    
    """)
    exec_command("""
create table acg_ad_统计_研发项目 as 
select *, null h单价调整金额（CA表）, null i研发借方额（E列加H列）, null j研发贷方额（FC表）, null k研发净额（I列减J列）, null l占比 
from stat_project    
    """)
    exec_command("""
update acg_ad_统计_研发项目 as a
   set i研发借方额（E列加H列）= 研发科目金额 + coalesce(h单价调整金额（CA表）, 0),
       j研发贷方额（FC表） = (select sum(i本币金额) from acg_fc_入库凭证追加行 b where b.e户号=a.项目编号 and b.c会计科目代码=a.研发科目编码)    
    """)
    exec_command("""
update acg_ad_统计_研发项目 as a
   set k研发净额（I列减J列） = i研发借方额（E列加H列） - coalesce(j研发贷方额（FC表）,0),
       l占比 = (i研发借方额（E列加H列） - coalesce(j研发贷方额（FC表）,0)) / (select sum( (i研发借方额（E列加H列） - coalesce(j研发贷方额（FC表）,0)) ) from acg_ad_统计_研发项目 b where b.项目编号=a.项目编号)
    """)
