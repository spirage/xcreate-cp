# -*- coding: UTF-8 -*-

from core.database import *


def process_voucher_recalculated():
    exec_command("drop table if exists vucher_recalculated")
    exec_command("""
create table vucher_recalculated as 
select 'FB_调整消耗凭证' a来源表, rowid b原行次, null d会计期, null e凭证日期, null f凭证号码, 凭证摘要 g凭证摘要, 借贷 h借贷, 会计科目代码 i会计科目代码, 会计科目中文名称 j会计科目中文名称, 户号 k户号, 户号名称 l户号名称, 参号 m参号, 参号名称 n参号名称, 本币金额 o原始金额, 数量 p原始数量, amount_adjusted r调整后金额, 数量 s调整后数量, r最终金额 t最终金额, s最终重量 u最终数量
from acg_fb_调整消耗凭证
union all
select 'FA_调整入库凭证' a来源表, rowid b原行次, null d会计期, null e凭证日期, null f凭证号码, 凭证摘要, 借贷, 会计科目代码, 会计科目中文名称, 户号, 户号名称, 参号, 参号名称, 本币金额, 数量, amount_adjusted, 数量, s最终金额, t最终重量
from acg_fa_调整入库凭证
union all
select 'FC_入库凭证追加行' a来源表, rowid b原行次, null d会计期, null e凭证日期, null f凭证号码, a凭证摘要, b借贷, c会计科目代码, d会计科目中文名称, e户号, f户号名称, g参号, h参号名称, null, null, null, null, i本币金额, j数量
from acg_fc_入库凭证追加行
union all 
select 'FD_调后转主营成本凭证' a来源表, rowid b原行次, 会计期, 凭证日期, 凭证号码, 凭证摘要, 借贷, 会计科目代码, 会计科目中文名称, 户号, 户号名称, 参号, 参号名称, b原金额, c原重量, null, null, 本币金额, 数量
from acg_fd_调后转主营成本凭证
    """)


def process_voucher_merged():
    exec_command("""
drop table if exists voucher_merged    
    """)
    exec_command("""
create table voucher_merged as 
select 'ACD' orig_table, orig_rowno orig_rowno, 会计期,凭证日期,凭证号码,凭证摘要,借贷,会计科目代码,会计科目中文名称,户号,户号名称,参号,参号名称,附加类别一,附加类别二,币种,本币金额,外币金额,数量
from voucher_splitted
where 凭证号码 not in (select voucher_no from tmp_semi_product_disposal)
union all
select 'ACG_'||substr(a来源表,1,2), b原行次, 
        case when a来源表 = 'FD_调后转主营成本凭证' then d会计期   else '202312' end,
        case when a来源表 = 'FD_调后转主营成本凭证' then e凭证日期 else (select 凭证日期 from voucher_entry where 凭证号码 in (select voucher_no from tmp_semi_product_disposal) limit 1) end,
        case when a来源表 = 'FD_调后转主营成本凭证' then f凭证号码 else (select voucher_no from tmp_semi_product_disposal limit 1) end,
        g凭证摘要,h借贷,i会计科目代码,j会计科目中文名称,k户号,l户号名称,m参号,n参号名称,null,null,null,t最终金额,null,u最终数量
from vucher_recalculated
where coalesce(f凭证号码,'') not in (select voucher_no from tmp_semi_product_disposal)
    """)
    exec_command("""
insert into voucher_merged
select 'VOUCHER_ENTRY' orig_table, "index" orig_rowno, 会计期,凭证日期,凭证号码,凭证摘要,借贷,会计科目代码,会计科目中文名称,户号,户号名称,参号,参号名称,附加类别一,附加类别二,币种,本币金额,外币金额,数量
from voucher_entry
where 凭证号码 in (select 凭证号码 from voucher_splitted) 
  and orig_rowno not in (select orig_rowno from voucher_splitted)    
    """)
    exec_command("""
update voucher_merged as a 
   set 凭证摘要 = ( select transfer from para_voucher_summary_transfer b where b.summary=a.凭证摘要 and b.account=a.会计科目中文名称 and (b.amount is null or (b.amount='>0' and a.本币金额>0) or (b.amount='<0' and a.本币金额<0)) )
 where exists ( select 1 from para_voucher_summary_transfer b where b.summary=a.凭证摘要 and b.account=a.会计科目中文名称 and (b.amount is null or (b.amount='>0' and a.本币金额>0) or (b.amount='<0' and a.本币金额<0)) )  
    """)


def process_stat_voucher():
    exec_command("""
drop table if exists stat_voucher    
    """)
    exec_command("""
create table stat_voucher as 
select 凭证号码,  
       ( select 凭证摘要 from voucher_merged b where b.orig_rowno=(select min(a.orig_rowno) from voucher_merged c where c.凭证号码=a.凭证号码) ) 摘要, 
       count(*) 行数, 
       sum(case when 借贷='1' then 本币金额 else 0 end) 借方金额, sum(case when 借贷='2' then 本币金额 else 0 end) 贷方金额, 
       round(sum(case when 借贷='1' then 本币金额 else 0 end) - sum(case when 借贷='2' then 本币金额 else 0 end),2) 借贷差
from voucher_merged a
group by 凭证号码    
    """)


def process_voucher_output():
    exec_command("""
drop table if exists voucher_output
    """)
    exec_command("""
create table voucher_output as 
select 凭证号码 序号（同一凭证头下的明细序号相同）, '1000' 账套, 会计期, null 凭证附件张数, substr(凭证号码, 1,2) 凭证分类, substr(凭证号码, 3,1) 凭证类型, 
       借贷 借贷标志（1借2贷）, 凭证摘要, 会计科目代码 会计科目代码（AAAA02A1页面查询）, 户号 户号（AAAAID01页面查询）, 参号, 附加类别一, 附加类别二, 币种 币种代码, 
       '0.0000' 记账汇率, 本币金额 本位币金额, 外币金额, 数量, null 数量单位, null 现金流量代码（AAAA10页面查询）
from voucher_merged
    """)


