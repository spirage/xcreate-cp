# -*- coding: UTF-8 -*-

from core.database import *


def process_voucher_recalculated():
    exec_command("drop table if exists voucher_recalculated")
    exec_command("""
create table voucher_recalculated as 
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
        --0630 修改 会计期处理方式
        case when a来源表 = 'FD_调后转主营成本凭证' then d会计期 else (select value from sys_config where key='sys.acc_period') end,
        case when a来源表 = 'FD_调后转主营成本凭证' then e凭证日期 else (select 凭证日期 from voucher_entry where 凭证号码 in (select voucher_no from tmp_semi_product_disposal) limit 1) end,
        case when a来源表 = 'FD_调后转主营成本凭证' then f凭证号码 else (select voucher_no from tmp_semi_product_disposal limit 1) end,
        g凭证摘要,h借贷,i会计科目代码,j会计科目中文名称,k户号,l户号名称,m参号,n参号名称,null,null,null,t最终金额,null,u最终数量
from voucher_recalculated
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


def process_voucher_rounded():
    # --0711 修改 所有拆分后凭证金额和数量进行舍入
    exec_command("drop table if exists voucher_rounded")
    exec_command("create table voucher_rounded as select * from voucher_merged")
    # 1 ACD
    exec_command("""
    update voucher_rounded as a
       set 本币金额 = round(本币金额,2)
    where orig_table  = 'ACD' 
      and exists (select 1 from voucher_entry ve where ve."index"=a.orig_rowno and ve.会计科目代码=a.会计科目代码 )
      --0926 修改 会计科目代码 not like '5301%' and  会计科目代码 not like '510101%'
        """)
    exec_command("""
    update voucher_rounded as a
       set 本币金额 = round(本币金额,2)
    where orig_table  = 'ACD' 
      and not exists (select 1 from voucher_entry ve where ve."index"=a.orig_rowno and ve.会计科目代码=a.会计科目代码 ) --0926 修改 会计科目代码 like '5301%'
      and not exists (select 1 from (select 户号, 本币金额 from (
                                                              select 户号, 本币金额, 
                                                                     row_number() over (partition by orig_rowno order by abs(本币金额) desc) rn 
                                                                from voucher_rounded a
                                                               where orig_table = 'ACD'
                                                                 and not exists (select 1 from voucher_entry ve where ve."index"=a.orig_rowno and ve.会计科目代码=a.会计科目代码 ) --0926 修改 会计科目代码 like '5301%'
                                                                 and orig_rowno=a.orig_rowno
                                                              ) where rn=1) x 
                                where x.户号=a.户号 and x.本币金额=a.本币金额 )    
        """)
    exec_command("""
    update voucher_rounded as a
       set 本币金额 = (case when instr( (select tag_special from voucher_entry b where b."index" = a.orig_rowno), 'C' ) > 0
                          then 
                            round( 
                                 (select 本币金额 from voucher_entry b where b."index" = a.orig_rowno)
                               - (select 本币金额 from voucher_rounded b where b.orig_rowno=a.orig_rowno and orig_table  = 'ACD' and exists (select 1 from voucher_entry ve where ve."index"=b.orig_rowno and ve.会计科目代码=b.会计科目代码 ) /*0926 修改 会计科目代码 not like '5301%' and  会计科目代码 not like '510101%'*/  )  
                               + (select sum(本币金额) from voucher_rounded b where b.orig_rowno=a.orig_rowno and b.orig_table  = 'ACD' and not exists (select 1 from voucher_entry ve where ve."index"=b.orig_rowno and ve.会计科目代码=b.会计科目代码 )/*b.会计科目代码 like '5301%'*/ and (b.户号<>a.户号 or b.本币金额<>a.本币金额))
                            , 2 ) * (-1)
                          else 
                            round( 
                                 (select 本币金额 from voucher_entry b where b."index" = a.orig_rowno)
                               - (select 本币金额 from voucher_rounded b where b.orig_rowno=a.orig_rowno and orig_table  = 'ACD' and exists (select 1 from voucher_entry ve where ve."index"=b.orig_rowno and ve.会计科目代码=b.会计科目代码 )/*会计科目代码 not like '5301%'*/)  
                               - (select sum(本币金额) from voucher_rounded b where b.orig_rowno=a.orig_rowno and b.orig_table  = 'ACD' and not exists (select 1 from voucher_entry ve where ve."index"=b.orig_rowno and ve.会计科目代码=b.会计科目代码 )/*b.会计科目代码 like '5301%'*/ and (b.户号<>a.户号 or b.本币金额<>a.本币金额))
                            , 2 )
                      end)                  
    where orig_table  = 'ACD' 
      and 会计科目代码 like '5301%'
      and exists (select 1 from (select 户号, 本币金额 from (
                                                          select 户号, 本币金额, 
                                                                 row_number() over (partition by orig_rowno order by abs(本币金额) desc) rn 
                                                            from voucher_rounded a
                                                           where orig_table = 'ACD'
                                                             and not exists (select 1 from voucher_entry ve where ve."index"=a.orig_rowno and ve.会计科目代码=a.会计科目代码 )/*会计科目代码 like '5301%'*/
                                                             and orig_rowno=a.orig_rowno
                                                          ) where rn=1) x 
                          where x.户号=a.户号 and x.本币金额=a.本币金额 )    
        """)
    # 2.1 ACG_FA ACG_FC
    exec_command("""
    update voucher_rounded 
       set 本币金额=round(本币金额,2)
     where orig_table in ('ACG_FA', 'ACG_FC')
       and 借贷='2'    
        """)
    exec_command("""
    update voucher_rounded as a
       set 本币金额=round(本币金额,2)
     where orig_table in ('ACG_FA', 'ACG_FC')
       and 借贷='1'  
       and not exists (select 1 from (select orig_table, orig_rowno from (
                                                                      select orig_table, orig_rowno, 本币金额, 
                                                                             row_number() over (partition by orig_table order by 本币金额 desc) rn 
                                                                        from voucher_rounded a
                                                                       where orig_table in ('ACG_FA', 'ACG_FC')
                                                                         and 借贷='1'
                                                                     ) where rn=1) x 
                                where x.orig_table=a.orig_table and x.orig_rowno=a.orig_rowno)    
        """)
    exec_command("""
    update voucher_rounded as a
       set 本币金额=round( (select sum(本币金额) from voucher_rounded b where b.orig_table=a.orig_table and b.借贷='2') - (select sum(本币金额) from voucher_rounded b where b.orig_table=a.orig_table and b.借贷='1' and b.orig_rowno<>a.orig_rowno) , 2 )
     where orig_table in ('ACG_FA', 'ACG_FC')
       and 借贷='1'  
       and exists (select 1 from (select orig_table, orig_rowno from (
                                                                      select orig_table, orig_rowno, 本币金额, 
                                                                             row_number() over (partition by orig_table order by 本币金额 desc) rn 
                                                                        from voucher_rounded a
                                                                       where orig_table in ('ACG_FA', 'ACG_FC')
                                                                         and 借贷='1'
                                                                     ) where rn=1) x 
                           where x.orig_table=a.orig_table and x.orig_rowno=a.orig_rowno)    
        """)
    # 2.2 ACG_FB
    exec_command("""
    update voucher_rounded 
       set 本币金额=round(本币金额,2)
     where orig_table = 'ACG_FB'
       and 借贷='1'    
        """)
    exec_command("""
    update voucher_rounded as a
       set 本币金额=round(本币金额,2)
     where orig_table = 'ACG_FB'
       and 借贷='2'  
       and not exists (select 1 from (select orig_table, orig_rowno from (
                                                                      select orig_table, orig_rowno, 本币金额, 
                                                                             row_number() over (partition by orig_table order by 本币金额 desc) rn 
                                                                        from voucher_rounded a
                                                                       where orig_table = 'ACG_FB'
                                                                         and 借贷='2'
                                                                     ) where rn=1) x 
                                where x.orig_table=a.orig_table and x.orig_rowno=a.orig_rowno)    
        """)
    exec_command("""
    update voucher_rounded as a
       set 本币金额=round( (select sum(本币金额) from voucher_rounded b where b.orig_table=a.orig_table and b.借贷='1') - (select sum(本币金额) from voucher_rounded b where b.orig_table=a.orig_table and b.借贷='2' and b.orig_rowno<>a.orig_rowno) , 2 )
     where orig_table = 'ACG_FB'
       and 借贷='2'  
       and exists (select 1 from (select orig_table, orig_rowno from (
                                                                      select orig_table, orig_rowno, 本币金额, 
                                                                             row_number() over (partition by orig_table order by 本币金额 desc) rn 
                                                                        from voucher_rounded a
                                                                       where orig_table = 'ACG_FB'
                                                                         and 借贷='2'
                                                                     ) where rn=1) x 
                           where x.orig_table=a.orig_table and x.orig_rowno=a.orig_rowno)    
        """)
    # 3 ACG_FD
    exec_command("""
    update voucher_rounded 
       set 本币金额=round(本币金额,2)
     where orig_table = 'ACG_FD'    
        """)
    # 4 调整数量
    exec_command("""
    update voucher_rounded as a
       set 数量 = round(数量,3)
    where orig_table  = 'ACD' 
      and exists (select 1 from voucher_entry ve where ve."index"=a.orig_rowno and ve.会计科目代码=a.会计科目代码 )/*会计科目代码 not like '5301%'*/    
        """)
    exec_command("""
    update voucher_rounded as a
       set 数量 = round(数量,3)
    where orig_table  = 'ACD' 
      and not exists (select 1 from voucher_entry ve where ve."index"=a.orig_rowno and ve.会计科目代码=a.会计科目代码 )/*会计科目代码 like '5301%'*/
      and not exists (select 1 from (select 户号, 数量 from (
                                                              select 户号, 数量, 
                                                                     row_number() over (partition by orig_rowno order by abs(数量) desc) rn 
                                                                from voucher_rounded a
                                                               where orig_table = 'ACD'
                                                                 and not exists (select 1 from voucher_entry ve where ve."index"=a.orig_rowno and ve.会计科目代码=a.会计科目代码 )/*会计科目代码 like '5301%'*/
                                                                 and orig_rowno=a.orig_rowno
                                                              ) where rn=1) x 
                                where x.户号=a.户号 and x.数量=a.数量 )    
        """)
    exec_command("""
    update voucher_rounded as a
       set 数量 = (case when instr( (select tag_special from voucher_entry b where b."index" = a.orig_rowno), 'C' ) > 0
                          then 
                            round( 
                                 (select 数量 from voucher_entry b where b."index" = a.orig_rowno)
                               - (select 数量 from voucher_rounded b where b.orig_rowno=a.orig_rowno and orig_table  = 'ACD' and exists (select 1 from voucher_entry ve where ve."index"=b.orig_rowno and ve.会计科目代码=b.会计科目代码 )/*会计科目代码 not like '5301%'*/)  
                               + (select sum(数量) from voucher_rounded b where b.orig_rowno=a.orig_rowno and b.orig_table  = 'ACD' and not exists (select 1 from voucher_entry ve where ve."index"=a.orig_rowno and ve.会计科目代码=a.会计科目代码 )/*b.会计科目代码 like '5301%'*/ and (b.户号<>a.户号 or b.数量<>a.数量))
                            , 3 ) * (-1)
                          else 
                            round( 
                                 (select 数量 from voucher_entry b where b."index" = a.orig_rowno)
                               - (select 数量 from voucher_rounded b where b.orig_rowno=a.orig_rowno and orig_table  = 'ACD' and exists (select 1 from voucher_entry ve where ve."index"=b.orig_rowno and ve.会计科目代码=b.会计科目代码 )/*会计科目代码 not like '5301%'*/)  
                               - (select sum(数量) from voucher_rounded b where b.orig_rowno=a.orig_rowno and b.orig_table  = 'ACD' and not exists (select 1 from voucher_entry ve where ve."index"=b.orig_rowno and ve.会计科目代码=b.会计科目代码 )/*b.会计科目代码 like '5301%'*/ and (b.户号<>a.户号 or b.数量<>a.数量))
                            , 3 )
                      end)                  
    where orig_table  = 'ACD' 
      and not exists (select 1 from voucher_entry ve where ve."index"=a.orig_rowno and ve.会计科目代码=a.会计科目代码 )/*会计科目代码 like '5301%'*/
      and exists (select 1 from (select 户号, 数量 from (
                                                          select 户号, 数量, 
                                                                 row_number() over (partition by orig_rowno order by abs(数量) desc) rn 
                                                            from voucher_rounded a
                                                           where orig_table = 'ACD'
                                                             and not exists (select 1 from voucher_entry ve where ve."index"=a.orig_rowno and ve.会计科目代码=a.会计科目代码 )/*会计科目代码 like '5301%'*/
                                                             and orig_rowno=a.orig_rowno
                                                          ) where rn=1) x 
                          where x.户号=a.户号 and x.数量=a.数量 )    
        """)
    exec_command("""
    update voucher_rounded set 数量=round(数量,3)
    where orig_table <> 'ACD'     
    """)


def process_stat_voucher():
    exec_command("drop table if exists stat_voucher")
    # 0711 修改 统计结果基于 voucher_rounded
    process_voucher_rounded()
    exec_command("""
create table stat_voucher as 
select 凭证号码,  
       ( select 凭证摘要 from voucher_rounded b where b.orig_rowno=(select min(a.orig_rowno) from voucher_rounded c where c.凭证号码=a.凭证号码) ) 摘要, 
       count(*) 行数, 
       --0630 修改 所有拆分后凭证金额和数量进行舍入 后调整
       --sum(case when 借贷='1' then 本币金额 else 0 end) 借方金额, 
       round(sum(case when 借贷='1' then 本币金额 else 0 end),2) 借方金额, 
       --sum(case when 借贷='2' then 本币金额 else 0 end) 贷方金额, 
       round(sum(case when 借贷='2' then 本币金额 else 0 end),2) 贷方金额, 
       round(sum(case when 借贷='1' then 本币金额 else 0 end) - sum(case when 借贷='2' then 本币金额 else 0 end),2) 借贷差
from voucher_rounded a
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
--from voucher_merged # 0711 修改 导出凭证 基于 voucher_rounded
  from voucher_rounded
    """)


def get_voucher_attach_vno():
    query = "select distinct 凭证号码 voucher_no from voucher_splitted where 会计科目代码 like '5301%'"
    cur = exec_query(query)
    rows = cur.fetchall()
    data = [dict(zip(tuple(column[0] for column in cur.description), row)) for row in rows]
    return data


def get_voucher_attach_tabtitle(voucher_no):
    if voucher_no is None or voucher_no == '':
        raise ValueError("参数值不在合理范围")
    query = """
select distinct voucher_no, sign, raccount_code, sign||raccount_code tab_id, case when sign='p' then raccount_name else raccount_name||'回收' end tab_name
from (
select 凭证号码 voucher_no,
       case when 本币金额>=0 then 'p' else 'n' end sign,
       会计科目代码 raccount_code,  
       replace('研发项目'||'-'||replace(会计科目中文名称, rtrim(会计科目中文名称, replace(会计科目中文名称, '-', '')), ''), '中间试验制造费', '试验费') raccount_name
from voucher_splitted
where 会计科目代码 like '5301%'
  and 凭证号码 = '"""+voucher_no+"""' )    
    """
    cur = exec_query(query)
    rows = cur.fetchall()
    data = [dict(zip(tuple(column[0] for column in cur.description), row)) for row in rows]
    return data


def get_voucher_attach_tabcontent(voucher_no, raccount_code, sign):
    if voucher_no is None or voucher_no == '' or raccount_code is None or not raccount_code.startswith('5301') or sign is None or sign not in ('p', 'n'):
        raise ValueError("参数值不在合理范围")
    query = """
select project_code, project_name, orig_ref_name cost_element, 
       case when orig_wt<>'0' then amount/(orig_amount/orig_wt) 
            when orig_ref_code in ('8000000','8000050','8000051','8000052','8000053','8000054','8000055','8000056','8000057','8000077','8000078') then amount/(orig_amount/720)
            else null end quantity,
       case when orig_wt<>'0' then orig_amount/orig_wt
            when orig_ref_code in ('8000000','8000050','8000051','8000052','8000053','8000054','8000055','8000056','8000057','8000077','8000078') then orig_amount/720
            else null end unit_price,
       case when orig_wt<>'0' then '吨'
            when orig_ref_code in ('8000000','8000050','8000051','8000052','8000053','8000054','8000055','8000056','8000057','8000077','8000078') then '小时'
            else null end unit,            
       amount rd_amount
from (
select 户号 project_code, 户号名称 project_name, 
       (select 参号 from voucher_entry b where b."index"=a.orig_rowno) orig_ref_code,
       (select 参号名称 from voucher_entry b where b."index"=a.orig_rowno) orig_ref_name,
       (select 本币金额 from voucher_entry b where b."index"=a.orig_rowno) orig_amount,
       (select 数量 from voucher_entry b where b."index"=a.orig_rowno) orig_wt,
       本币金额 amount
from voucher_splitted a
where 会计科目代码 like '5301%'
  and 凭证号码 = '"""+voucher_no+"""' 
  and 会计科目代码 = '"""+raccount_code+"""'
  and ( ('"""+sign+"""'='p' and 本币金额>=0) or ('"""+sign+"""'='n' and 本币金额<0) )  )  
    """
    cur = exec_query(query)
    rows = cur.fetchall()
    data = [dict(zip(tuple(column[0] for column in cur.description), row)) for row in rows]
    return data
