# -*- coding: UTF-8 -*-
from core.database import *
import random
import itertools


def generate_random_with_bias(weights):
    choices = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    return random.choices(choices, weights=weights, k=1)[0]


def generate_random_integers(count, sum_value):
    if count <= 0 or sum_value <= 0 or sum_value > count*8:
        raise ValueError("参数值不在合理范围")
    ratio = sum_value/(count*8)
    weights = [2 * 4, 1, 1, 1, 4, 4, 16 * 1, 16 * 1, 16 * 2, 0]
    weights[0] = 2 * 4 / ratio
    weights[1] = 1
    weights[2] = 1
    weights[3] = 1 * 1 * 1 / ratio
    weights[4] = 1 * 1 * 1 / ratio
    weights[5] = 1 * 1 * 1 / ratio
    weights[6] = 16 * 2 * ratio
    weights[7] = 16 * 2 * ratio
    weights[8] = 16 * 16 * ratio
    weights[9] = 0
    # print(weights)
    numbers = []
    i = 0
    while i < count-1:
        numbers.append(generate_random_with_bias(weights))
        i += 1
        if i == count-1 and (sum_value - sum(numbers) < 0 or sum_value - sum(numbers) > 8):
            numbers = []
            i = 0
    numbers.append(sum_value - sum(numbers))
    return numbers


def gen_staff_manhour_detail():
    cur = exec_query("select count(*) from sys_workday")
    workday_count = cur.fetchone()[0]
    workdays = []
    if workday_count > 0:
        cur = exec_query("select day from sys_workday")
        rows = cur.fetchall()
        workdays = [int(row[0]) for row in rows]
    manhour_detail = []
    cur = exec_query("select staff_no, project_code, manhour_total from tmp_manhour_total")
    for row in cur.fetchall():
        random_integers = generate_random_integers(workday_count, row[2])
        day_hours = list(zip(workdays, random_integers))
        man_hours = [[row[0], row[1], day, hour] for day, hour in day_hours]
        manhour_detail += man_hours

    df = pd.DataFrame(manhour_detail, columns=['staff_no', 'project_code', 'day', 'hour'])
    df.to_sql('tmp_manhour_detail', conn, if_exists="replace", index=False)


def gen_voucher_entry():
    logger.info("acc_凭证表加工_1生成")
    exec_command("create index if not exists ix_orig_voucher_entry_index ON orig_voucher_entry(\"index\")")
    exec_command("create index if not exists ix_orig_voucher_entry_voucherno ON orig_voucher_entry(凭证号码)")
    exec_command("create index if not exists ix_orig_voucher_entry_refno ON orig_voucher_entry(参号)")
    exec_command("drop table if exists voucher_entry")
    exec_command("""
create table voucher_entry as
select *, null tag_special from orig_voucher_entry a 
where exists ( select 1 from orig_voucher_entry b 
                where b.凭证号码=a.凭证号码 
                  and b.凭证号码 is not null 
                  and trim(b.凭证号码) <> '' 
                  --0530 修改
                  and b.凭证状态 <> '50'
                  and 参号 in (select distinct substr(成本科目,1,instr(成本科目,'-')-1) from orig_product_cost )
             )
    """)
    exec_command("create index if not exists ix_voucher_entry_summary on voucher_entry(凭证摘要)")
    exec_command("create index if not exists ix_voucher_entry_acctitle on voucher_entry(会计科目代码)")
    exec_command("create index if not exists ix_voucher_entry_accno on voucher_entry(户号)")
    exec_command("create index if not exists ix_voucher_entry_refno on voucher_entry(参号)")
    # 0605 修改 在凭证表加工时清空研发分配人工设置系数，保证第一次运行process_map_ccenter_caccount_svoucher时系数都是1
    exec_command("delete from para_ba_rd_split_ratio")


def tag_voucher_entry():
    logger.info("acc_凭证表加工_2标记")
    exec_command("""
update voucher_entry as a 
set tag_special='A'
where exists ( select 1 from (select group_concat(distinct pzhms) pzhms from (
select 会计科目代码, 户号, 参号, group_concat(distinct 凭证号码) pzhms, round(sum(本币金额),2) hjje
  from voucher_entry b where b.凭证摘要 like '%红冲%' or b.凭证摘要 like '%暂估%'
group by 会计科目代码, 户号, 参号
having round(sum(本币金额),2) = 0 ) ) where instr(pzhms, a.凭证号码)>0 )
    """)
    exec_command("""
update voucher_entry as a 
set tag_special=case when tag_special is null then 'B' else tag_special||',B' end
where exists ( select 1 from (select group_concat(distinct pzhms) pzhms from (
select 会计科目代码, 户号, 参号, group_concat(distinct 凭证号码) pzhms, round(sum(本币金额),2) hjje
  from voucher_entry b where b.凭证摘要 like '%红冲%' or b.凭证摘要 like '%暂估%'
group by 会计科目代码, 户号, 参号
having round(sum(本币金额),2) <> 0 ) ) where instr(pzhms, a.凭证号码)>0 )
    """)
    exec_command("""
update
    voucher_entry as a
    set  tag_special = case when tag_special is null then 'C' else tag_special||',C' end 
    where a.借贷 = '2'
    and (a.会计科目中文名称 like '生产成本%' or a.会计科目中文名称 like '制造费用%')
    and instr(a.会计科目中文名称, '结转') = 0
    """)
    exec_command("""
update voucher_entry as a 
set tag_special=case when tag_special is null then 'D' else tag_special||',D' end
where a.户号 in (select code from code_cost_center where transfer is not null) 
    """)


def gen_tmp_special_retrospect():
    logger.info("生成tmp_special_retrospect")
    exec_command("drop table if exists tmp_special_retrospect")
    exec_command("""
create table tmp_special_retrospect as  
select distinct 参号 cost_account_code, 参号名称 cost_account_name, 户号 cost_center_code, 户号名称 cost_center_name, b.transfer cost_center_transfer
from orig_voucher_entry a,
     code_cost_center b
where a.户号=b.code
  and a.借贷='2'
  and a.参号 like '8%'
  and b.transfer is not null  
    """)


# 0604 修改 不需人工该选择，自动选择摘要含有“半成品产出抛帐”的最后一次（行号最大）凭证号，将其放入tmp_semi_product_disposal
def gen_tmp_semi_product_disposal():
    logger.info("生成tmp_semi_product_disposal")
    exec_command("drop table if exists tmp_semi_product_disposal")
    exec_command("""
create table tmp_semi_product_disposal as 
select 凭证号码 voucher_no
from orig_voucher_entry 
where "index" = (select max("index") 
                   from orig_voucher_entry 
                  where instr(凭证摘要,'半成品产出抛帐')>0)    
    """)


# 根据0606测试文档 0615 讨论确定 0618 修改 amount_incurred发生凭证金额 和 voucher_incurred发生凭证 去掉判断条件 "不能是之前后台表sheet 凭证导入中记录的凭证号(ACH1000008)"
def process_map_ccenter_caccount_svoucher():
    logger.info("acc_产副品表与凭证表分析")
    gen_tmp_special_retrospect()
    gen_tmp_semi_product_disposal()
    exec_command("drop table if exists map_ccenter_caccount_voucher")
    exec_command("""
create table map_ccenter_caccount_voucher as
select a.cost_center_code, a.cost_center_name, a.cost_account_code, a.cost_account_name, a.total_cost, 
       b.amount_incurred, b.voucher_incurred, b.amount_retrospect, b.voucher_retrospect, b.amount_carryforward
from 
(select substr(成本中心,1,instr(成本中心,'-')-1) cost_center_code, substr(成本中心,instr(成本中心,'-')+1) cost_center_name,
       substr(成本科目,1,instr(成本科目,'-')-1) cost_account_code, substr(成本科目,instr(成本科目,'-')+1) cost_account_name,
       round(sum(本月总成本),2) total_cost
from orig_product_cost
group by 1,2,3,4
) a left join
(
select case when instr(a.tag_special,'D')>0 then (select c.transfer from code_cost_center c where c.code=a.户号)
       else 户号 end cost_center_code,
       参号 cost_account_code, 
       case when 参号 in ('1011008','1011009') then null  -- 0618 修改 排除 主原料-焦化除尘灰费用 和 主原料-冲焦化除尘灰费用
            else round(sum(case when /* 0618 修改 凭证号码 not in (select voucher_no from tmp_semi_product_disposal) and*/ 借贷='1' then 本币金额 
                                when 借贷='2' and a.tag_special='C' then -1*本币金额  -- 0618 新增 C类凭证处理
                                else null 
                           end),2)
       end amount_incurred, 
       case when 参号 in ('1011008','1011009') then null  -- 0618 修改 排除 主原料-焦化除尘灰费用 和 主原料-冲焦化除尘灰费用
            else group_concat(case when /* 0618 修改 凭证号码 not in (select voucher_no from tmp_semi_product_disposal) and*/ 借贷='1' then "index" 
                                   when 借贷='2' and a.tag_special='C' then "index"  -- 0618 新增 C类凭证处理
                              end)
       end voucher_incurred,
       round(sum(case when exists (select 1 from tmp_special_retrospect x where x.cost_center_code=a.户号 and x.cost_account_code=a.参号) 
                      then (select sum(本币金额) from voucher_entry b 
                             where (b.户号=a.户号 or b.户号=(select cost_center_transfer from tmp_special_retrospect x where x.cost_center_code=a.户号))
                               and b.参号 in ('5100000', '5200000')
                               and b.借贷 = '1')
                 else null end),2) amount_retrospect,
       group_concat(case when exists (select 1 from tmp_special_retrospect x where x.cost_center_code=a.户号 and x.cost_account_code=a.参号) 
                    then (select group_concat("index") from voucher_entry b 
                           where (b.户号=a.户号 or b.户号=(select cost_center_transfer from tmp_special_retrospect x where x.cost_center_code=a.户号))
                             and b.参号 in ('5100000', '5200000')
                             and b.借贷 = '1') 
                    end) voucher_retrospect,
       case when 参号 in ('1011008','1011009') then null  -- 0618 修改 排除 主原料-焦化除尘灰费用 和 主原料-冲焦化除尘灰费用
            else round(sum(case when 凭证号码 in (select voucher_no from tmp_semi_product_disposal) and 借贷='2' then 本币金额 else null end),2)
       end amount_carryforward
from voucher_entry a
group by 1,2
) b on a.cost_center_code = b.cost_center_code and a.cost_account_code = b.cost_account_code
    """)
    exec_command("drop table if exists map_product_account")
    exec_command("""
--ACC_产副品表加工
--根据0606测试文档 0615讨论结果 0618 修改
create table map_product_account as 
select substr(成本中心, 1, instr(成本中心, '-')-1) cost_center_code, substr(成本中心, instr(成本中心, '-')+1) cost_center_name, 
             substr(产副品代码, 1, instr(产副品代码, '-')-1) product_code, substr(产副品代码, instr(产副品代码, '-')+1) product_name, 
             辅助核算对象 ba_object_sub_1, 
             substr(成本科目, 1, instr(成本科目, '-')-1) cost_account_code, substr(成本科目, instr(成本科目, '-')+1) cost_account_name,
             本月产量 month_output , 本月总成本 month_cost, 本月总耗 month_consume
        from orig_product_cost
       where substr(成本科目, instr(成本科目, '-')+1) not like '主原料-%'
          --0618 修改
          or instr('主原料-铁块、主原料-外购邯宝铁水、主原料-外购股份铁水、主原料-股份自产压块、主原料-钢渣大渣块、主原料-邯宝烧结矿',substr(成本科目,instr(成本科目,'-')+1))>0
          or substr(成本科目,instr(成本科目,'-')+1) in (select name from code_cost_account where source_type='直接支用-外购半成品')
    """)
    exec_command("drop table if exists map_project_product_account")
    exec_command("""
create table map_project_product_account as 
select a.project_code, a.cost_center_code, b.cost_center_name, a.product_code, b.product_name, a.ba_object_sub_1, b.cost_account_code, b.cost_account_name,
       a.wt, b.month_output, b.month_cost, b.month_consume,
       b.month_cost     / b.month_output * a.wt rd_amount,
       b.month_consume   / b.month_output * a.wt rd_consume,
       null ratio_adjust
from map_project_product a,
     map_product_account b
  on a.cost_center_code = b.cost_center_code
 and a.product_code = b.product_code
 and a.ba_object_sub_1 = b.ba_object_sub_1
    """)
    exec_command("""
update map_project_product_account as a
set ratio_adjust = coalesce ( (select ratio_adjust from para_ba_rd_split_ratio b where b.cost_account_code=a.cost_account_code and b.cost_account_code = a.cost_account_code and b.project_code = a.project_code), 1)
    """)
    exec_command("drop table if exists map_product_account_rd")
    exec_command("""
create table map_product_account_rd as 
select a.cost_center_code, a.cost_center_name, a.product_code, a.product_name, a.ba_object_sub_1, a.cost_account_code, a.cost_account_name, a.month_output, a.month_cost, a.month_consume, b.rd_amount, b.rd_consume
from map_product_account a
left join 
( select cost_center_code, product_code, ba_object_sub_1, cost_account_code, sum(rd_amount) rd_amount, sum(rd_consume) rd_consume
    from map_project_product_account a     
   group by 1,2,3,4) b
on a.cost_center_code=b.cost_center_code and a.product_code=b.product_code and a.ba_object_sub_1=b.ba_object_sub_1 and a.cost_account_code=b.cost_account_code
    """)
    exec_command("drop table if exists map_ccenter_caccount_svoucher")
    exec_command("""
create table map_ccenter_caccount_svoucher as 
select row_number() over() rowno, a.*, b.rd_amount, b.rd_consume, 
       null voucher_type, null voucher_selected, null amount_selected, null rd_ratio
from map_ccenter_caccount_voucher a left join
(
select cost_center_code, cost_account_code, sum(rd_amount) rd_amount, sum(rd_consume) rd_consume
from map_product_account_rd
group by 1, 2
) b
on  a.cost_center_code = b.cost_center_code
and a.cost_account_code = b.cost_account_code
    """)
    exec_command("""
update map_ccenter_caccount_svoucher
set voucher_type = case when cost_account_code in (select cost_account_code from para_reimbursement_preparation) then 'TG-BZD'
                        when cost_account_code in ('5100000','5200000') then 'TS-1'
                        when rd_amount = 0 then 'TG-LZ'
                        when voucher_retrospect is not null then 'ZS-1'                            
                        else 'PT' -- 0629 修改 ||(length(voucher_incurred) - length(replace(voucher_incurred,',','')) + 1)
                   end                   
where rd_amount is not null
    """)
    exec_command("""
update map_ccenter_caccount_svoucher as a 
set voucher_type = 'TS-2'
where exists (select 1 from map_ccenter_caccount_svoucher b 
               where b.voucher_type = 'TS-1' 
                 and b.cost_center_code = a.cost_center_code 
                 and a.cost_account_code='8000050')
    """)
    exec_command("""
update map_ccenter_caccount_svoucher as a
set voucher_selected = (select row_no from 
                       (select row_no, row_number()over(order by sign(amount) desc,abs(amount) desc) rn from 
                        --0630 修改 C类凭证金额取反
                        (
                        select "index" row_no, case when instr(tag_special,'C')>0 then -1*本币金额 else 本币金额 end amount from voucher_entry                         
                        where instr(','||a.voucher_incurred||',', ','||"index"||',') > 0
                          and instr(coalesce(tag_special,'null'),'A')=0
                          and instr(coalesce(tag_special,'null'),'B')=0
                        )
                       ) where rn = 1),
    amount_selected =  (select amount from 
                       (select amount, row_number()over(order by sign(amount) desc,abs(amount) desc) rn from 
                        --0630 修改 C类凭证金额取反
                        (
                        select case when instr(tag_special,'C')>0 then -1*本币金额 else 本币金额 end amount from voucher_entry                         
                        where instr(','||a.voucher_incurred||',', ','||"index"||',') > 0
                          and instr(coalesce(tag_special,'null'),'A')=0
                          and instr(coalesce(tag_special,'null'),'B')=0
                        )
                       ) where rn = 1)
where cost_account_code not in ('5100000','5200000')
    """)
    exec_command("""
update map_ccenter_caccount_svoucher as a
set voucher_selected = (select voucher_selected from map_ccenter_caccount_svoucher b
                        where b.cost_center_code=a.cost_center_code
                          and b.cost_account_code='8000050'),
    amount_selected =  (select amount_incurred from map_ccenter_caccount_svoucher b
                        where b.cost_center_code=a.cost_center_code
                          and b.cost_account_code='8000050')
where cost_account_code in ('5100000','5200000')
    """)
    exec_command("""
update map_ccenter_caccount_svoucher as a 
set rd_ratio = rd_amount/amount_selected
where coalesce(amount_selected,0)<>0
    """)


def get_voucher_detail(voucher_index):
    if voucher_index is None or voucher_index < 0:
        raise ValueError("参数值不在合理范围")
    query = "select * from voucher_entry where \"index\" = " + str(voucher_index)
    cur = exec_query(query)
    row = cur.fetchone()
    data = dict(zip(tuple(column[0] for column in cur.description), row))
    return data


def get_voucher_by_no(voucher_no):
    if voucher_no is None or voucher_no == '':
        raise ValueError("参数值不在合理范围")
    query = "select * from voucher_entry where 凭证号码 = '" + str(voucher_no) + "'"
    cur = exec_query(query)
    rows = cur.fetchall()
    data = [dict(zip(tuple(column[0] for column in cur.description), row)) for row in rows]
    return data


# 根据0606测试文档 0615讨论确定 导出当前系数人工设置表时，过滤掉报制单类成本科目 0618 修改
def gen_para_ba_rd_split_ratio():
    logger.info("生成para_ba_rd_split_ratio")
    exec_command("drop table if exists para_ba_rd_split_ratio")
    exec_command("""
create table para_ba_rd_split_ratio as
select a.project_code, (select 项目名称 from para_project x where x.项目编码=a.project_code limit 1) project_name,
       a.cost_center_code, a.cost_center_name,
       a.cost_account_code, a.cost_account_name,
       round(sum(a.rd_amount),2) rd_amount, 
       a.ratio_adjust
from map_project_product_account a
where cost_account_code not in (select cost_account_code from para_reimbursement_preparation)
group by 1,3,4,5,6,8
    """)


def select_voucher(rowno, voucher_index):
    if rowno is None or rowno < 0 or voucher_index is None or voucher_index < 0:
        raise ValueError("参数值不在合理范围")
    query = "select voucher_incurred, cost_account_code, voucher_selected from map_ccenter_caccount_svoucher where rowno = " + str(rowno)
    row = exec_query(query).fetchone()
    voucher_incurred = row[0]
    cost_account_code = row[1]
    voucher_selected = row[2]
    if voucher_incurred is None or voucher_incurred == "" or str(voucher_index) not in voucher_incurred:
        raise ValueError("发生凭证异常或参数凭证index不在发生凭证范围")
    if ',' not in voucher_incurred or voucher_selected == str(voucher_index):
        raise ValueError("发生凭证仅有一条记录或者已选择凭证已是参数中凭证")
    if cost_account_code in ('5100000', '5200000'):
        raise ValueError("会计科目为 5100000-人员费用-职工薪酬 或 5200000-人员费用-福利费 的 是特殊凭证，不能进行凭证选择操作")
    exec_command("""
update map_ccenter_caccount_svoucher 
   set voucher_selected = """ + str(voucher_index) + """,
       --0630 修改 C类凭证金额取反
       amount_selected = (select case when instr(tag_special,'C')>0 then -1*本币金额 else 本币金额 end from voucher_entry where "index" = """ + str(voucher_index) + """),
       rd_ratio = rd_amount/(select case when instr(tag_special,'C')>0 then -1*本币金额 else 本币金额 end from voucher_entry where "index" = """ + str(voucher_index) + """)
 where rowno = """ + str(rowno))


def process_voucher_splitted():
    logger.info("acc_处理拆分凭证")
    exec_command("drop table if exists tmp_svoucher_project_raccount")
    exec_command("""
create table tmp_svoucher_project_raccount as
select x.voucher_selected, x.cost_account_code, x.voucher_type, ratio_retrospect, project_code, ratio_adjust, project_ramount, project_rconsume, ratio_account, raccount_code, raccount_name  from 
(
select a.voucher_selected, a.cost_account_code, a.voucher_type, a.amount_retrospect/a.amount_incurred ratio_retrospect, b.project_code, b.ratio_adjust, sum(b.rd_amount) project_ramount, sum(b.rd_consume) project_rconsume
from map_ccenter_caccount_svoucher a,
     map_project_product_account b
where a.voucher_type in ('PT','ZS-1','TS-1','TS-2')
  and a.cost_center_code = b.cost_center_code
  and a.cost_account_code = b.cost_account_code
group by 1,2,3,4,5,6
) x,
(
select a.voucher_selected, a.cost_account_code, a.voucher_type, a.ratio_account, b.对应的费用化研发科目编码 raccount_code, b.对应的费用化研发科目名称 raccount_name, b.对应的资本化研发科目编码 rcacount_code, b.对应的资本化研发科目名称 rcaccount_name from 
(
with x as (
select a.voucher_selected, a.cost_account_code, a.voucher_type, b."index" index_retrospect, b.本币金额 amount, b.会计科目中文名称 account_name, 
       case when a.voucher_type='ZS-1' and ((b.会计科目中文名称 like '制造费用-职工薪酬-%' and  b.会计科目中文名称 <> '制造费用-职工薪酬-职工福利费') or b.会计科目中文名称='制造费用-职工福利费-交通补贴') then '人员费用-职工薪酬'
            when a.voucher_type='ZS-1' then '人员费用-福利费' 
            when a.voucher_type in ('TS-1','TS-2','PT') then a.cost_account_name
       end cost_account_name
from map_ccenter_caccount_svoucher a,
     voucher_entry b
where (a.voucher_type='ZS-1' and instr(','||a.voucher_retrospect||',', ','||b."index"||',')>0) 
   or (a.voucher_type in ('PT','TS-2') and a.voucher_selected=b."index")
   or (a.voucher_type='TS-1' and instr(','||a.voucher_incurred||',', ','||b."index"||',')>0)
) select voucher_selected, cost_account_code, voucher_type, account_name, cost_account_name, amount/(select sum(amount) from x as x2 where x2.voucher_selected=x.voucher_selected and x2.cost_account_code=x.cost_account_code and x2.voucher_type=x.voucher_type) ratio_account from x
) a, para_caccount_raccount b 
where a.account_name = b.成本科目内容 
  and a.cost_account_name = b.成本元素
  and b.是否拆分 = '是'
) y
where x.voucher_selected = y.voucher_selected and x.cost_account_code = y.cost_account_code and x.voucher_type = y.voucher_type
order by x.voucher_selected, x.cost_account_code, project_code, raccount_code
    """)
    exec_command("drop table if exists map_svoucher_project_raccount")
    exec_command("""
create table map_svoucher_project_raccount as 
select voucher_selected, cost_account_code, voucher_type, project_code, ratio_adjust, project_ramount, project_rconsume, raccount_code, raccount_name, sum(project_ramount*coalesce(ratio_retrospect,1)*ratio_account) account_ramount, sum(project_rconsume*coalesce(ratio_retrospect,1)*ratio_account) account_rconsume
from tmp_svoucher_project_raccount
group by 1,2,3,4,5,6,7,8,9
    """)
    exec_command("""
insert into map_svoucher_project_raccount
select voucher_selected, cost_account_code, voucher_type, project_code, ratio_adjust, project_ramount, project_rconsume, '5301010020020' raccount_code, '研发支出-费用化支出-自筹资金-中间试验制造费' raccount_name, project_ramount - sum(account_ramount) account_ramount, project_rconsume - sum(account_ramount) account_rconsume
from map_svoucher_project_raccount
where voucher_type = 'ZS-1'
group by 1,2,3,4,5,6,7
    """)
    exec_command("update map_svoucher_project_raccount set account_ramount = account_ramount*coalesce(ratio_adjust,1)")
    exec_command("drop table if exists tmp_voucher_splitted")
    exec_command("""
create table tmp_voucher_splitted as 
select voucher_selected orig_rowno, cost_account_code orig_caccount, voucher_type orig_vtype, ratio_adjust, 
       b.会计期, b.凭证日期, b.凭证号码, b.凭证摘要, 
       --0630 修改 C类凭证拆出的凭证借贷取1
       case when instr(b.tag_special,'C')>0 then '1' else b.借贷 end 借贷, 
       a.raccount_code 会计科目代码, a.raccount_name 会计科目中文名称, a.project_code 户号, (select 项目名称 from para_project x where x.项目编码=a.project_code limit 1) 户号名称,
       null 参号, null 参号名称, null 附加类别一, null 附加类别二,
       b.币种, 
       --0630 修改 所有拆分后凭证金额和数量进行舍入
       a.account_ramount 本币金额,
       --round(a.account_ramount,2) 本币金额, 
       0 外币金额, 
       --0630 修改 所有拆分后凭证金额和数量进行舍入
       a.account_rconsume 数量
       --round(a.account_rconsume,4) 数量
  from map_svoucher_project_raccount a,
       voucher_entry b
where a.voucher_selected = b."index"
    """)
    exec_command("""
insert into tmp_voucher_splitted
select a."index", null, null, null,
       a.会计期, a.凭证日期, a.凭证号码, a.凭证摘要, a.借贷, 
       a.会计科目代码, a.会计科目中文名称, a.户号, a.户号名称,
       a.参号, a.参号名称, a.附加类别一, a.附加类别二,
       a.币种, 
       --0630 修改 C类凭证金额取反       
       case when instr(a.tag_special,'C')>0 then -1 *( -1 * a.本币金额 - b.amount_splitted )
            else (a.本币金额 - b.amount_splitted) end, 
       a.外币金额, 
       --0630 修改 C类凭证金额取反
       case when instr(a.tag_special,'C')>0 then -1 *( -1 * a.数量 - b.wt_splitted )
            else (a.数量 - b.wt_splitted) end
from voucher_entry a,
     (select orig_rowno, sum(本币金额) amount_splitted, sum(数量) wt_splitted from tmp_voucher_splitted group by orig_rowno) b
where a."index" = b.orig_rowno
    """)
    exec_command("drop table if exists voucher_splitted")
    exec_command("""    
create table voucher_splitted as 
select row_number()over(order by orig_rowno, orig_caccount, orig_vtype) rowno, * 
from tmp_voucher_splitted
    """)


# 0704 新增 王坤群里联系增加成本科目和研发科目保留首次分配时数据需求
def process_stat_raccount_orig():
    exec_command("drop table if exists stat_raccount_orig")
    exec_command("""
create table stat_raccount_orig as 
select raccount_code 研发科目编码, raccount_name 研发科目名称, sum(account_ramount) 研发金额
from map_svoucher_project_raccount
group by 1,2
    """)


def process_stat_raccount():
    exec_command("drop table if exists stat_raccount")
    exec_command("""
create table stat_raccount as 
select raccount_code 研发科目编码, raccount_name 研发科目名称, sum(account_ramount) 研发金额
from map_svoucher_project_raccount
group by 1,2
    """)


def process_stat_project_orig():
    exec_command("drop table if exists stat_project_orig")
    exec_command("""
create table stat_project_orig as
select project_code 项目编号, (select 项目名称 from para_project b where b.项目编码=a.project_code limit 1) 项目名称,  raccount_code 研发科目编码, raccount_name 研发科目名称, sum(account_ramount) 研发科目金额, sum(account_ramount) / sum( sum(account_ramount) ) over (partition by project_code) 比例
from map_svoucher_project_raccount a
group by 1,2,3,4
    """)


def process_stat_project():
    exec_command("drop table if exists stat_project")
    exec_command("""
create table stat_project as
select project_code 项目编号, (select 项目名称 from para_project b where b.项目编码=a.project_code limit 1) 项目名称,  raccount_code 研发科目编码, raccount_name 研发科目名称, sum(account_ramount) 研发科目金额, sum(account_ramount) / sum( sum(account_ramount) ) over (partition by project_code) 比例
from map_svoucher_project_raccount a
group by 1,2,3,4
    """)


# 0704 新增 王坤群里联系增加成本科目和研发科目保留首次分配时数据需求
def process_stat_caccount_orig():
    exec_command("drop table if exists stat_caccount_orig")
    exec_command("""
create table stat_caccount_orig as
select cost_account_code 成本科目代码,  cost_account_name 成本科目名称, cost_center_code 成本中心代码, cost_center_name 成本中心名称,  
       (select sum(account_ramount) from map_svoucher_project_raccount b where b.voucher_selected=a.voucher_selected group by b.voucher_selected) 研发金额,
       (select sum(account_rconsume) from map_svoucher_project_raccount b where b.voucher_selected=a.voucher_selected group by b.voucher_selected) 研发消耗
from map_ccenter_caccount_svoucher a
where coalesce(a.rd_amount,0) <> 0 
  and a.voucher_type not like 'TG-%'
  and a.voucher_type not like 'TS-1'
  and instr(a.cost_center_name, '来料加工') = 0
  and instr(a.cost_center_name, '外销') = 0
group by 1,2,3,4
    """)


def process_stat_caccount():
    exec_command("drop table if exists stat_caccount")
    exec_command("""
create table stat_caccount as
select cost_account_code 成本科目代码,  cost_account_name 成本科目名称, cost_center_code 成本中心代码, cost_center_name 成本中心名称,  
       (select sum(account_ramount) from map_svoucher_project_raccount b where b.voucher_selected=a.voucher_selected group by b.voucher_selected) 研发金额,
       (select sum(account_rconsume) from map_svoucher_project_raccount b where b.voucher_selected=a.voucher_selected group by b.voucher_selected) 研发消耗
from map_ccenter_caccount_svoucher a
where coalesce(a.rd_amount,0) <> 0 
  and a.voucher_type not like 'TG-%'
  and a.voucher_type not like 'TS-1'
  and instr(a.cost_center_name, '来料加工') = 0
  and instr(a.cost_center_name, '外销') = 0
group by 1,2,3,4
    """)


def process_stat_after_adjusted_left():
    exec_command("drop table if exists acd_系数设置后指标统计_左")
    exec_command("""
create table acd_系数设置后指标统计_左 as
select a.cost_center_code, a.cost_center_name, a.product_code, a.product_name, 
       b.ratio_recycle, 
       --0630 调整首笔投入算法，增加标识研发项目非空条件
       ( b.ratio_recycle * (select coalesce(sum(mat_wt),0) from mat_track_detail where account_title_item='31' and (cost_center<>' ' and cost_center<>'' and cost_center is not null) and project_code is not null) * (select ratio_recycle from para_recycle where cost_center_code='all' and product_code='all') ) wt_recycle,
       null amount_recycle,
       (select mat_wt from aca_dd x where x.cost_center=a.cost_center_code and x.product_code=a.product_code) wt_inventory_transfer,
       null amount_inventory_transfer,
       c.month_output, c.month_cost, case when c.month_output=0 then 0 else c.month_cost/c.month_output end unit_price,
       b.ratio_discount, 
       case when c.month_output=0 then 0 else c.month_cost/c.month_output end * b.ratio_discount unit_price_discount,
       b.ratio_adjust, 
       case when c.month_output=0 then 0 else c.month_cost/c.month_output end * b.ratio_adjust   unit_price_adjust
from map_product_account_rd a 
left join para_recycle b on a.cost_center_code=b.cost_center_code and a.product_code=b.product_code
left join (select cost_center_code, product_code, sum(case when rn=1 then month_output else 0 end) month_output, sum(month_cost) month_cost 
             --0630 修改 数据源从产副品加工调整为成本中心产副品构成
             from ( select substr(成本中心,1,instr(成本中心,'-')-1) cost_center_code, substr(产副品代码,1,instr(产副品代码,'-')-1) product_code, 辅助核算对象 ba_object_sub_1, 本月产量 month_output, 本月总成本 month_cost, row_number() over(partition by substr(成本中心,1,instr(成本中心,'-'-1)), substr(产副品代码,1,instr(产副品代码,'-')-1), 辅助核算对象) rn from orig_product_cost ) 
             group by 1,2
          ) c on a.cost_center_code=c.cost_center_code and a.product_code=c.product_code
group by 1,2,3,4
    """)
    exec_command("""
    update acd_系数设置后指标统计_左
set amount_recycle = wt_recycle * unit_price_discount,
    amount_inventory_transfer = wt_inventory_transfer * unit_price_adjust
    """)


# 根据0606 测试文档 0615讨论确定 0618 修改 改为orig_product_cost左关联map_project_product_account
def process_stat_after_adjusted_right():
    exec_command("drop table if exists acd_系数设置后指标统计_右")
    exec_command("""
create table acd_系数设置后指标统计_右 as
select case when substr(a.成本科目,1,1) in ('1','2') and instr(a.成本科目, '合金料')>0 then '合金料'
            when substr(a.成本科目,1,1) in ('1','2') and substr(a.成本科目,1,instr(a.成本科目,'-')-1) in ('1031002', '1091001', '1031001') then '半成品'
            when substr(a.成本科目,1,1) in ('1','2') then '主原料（不含半成品）'
            when substr(a.成本科目,1,1) = '3' then '辅助材料'
            when substr(a.成本科目,1,1) = '4' then '能源介质'
            when substr(a.成本科目,1,1) = '5' and instr(a.成本科目, '职工薪酬')>0 then '人员费用-职工薪酬'
            when substr(a.成本科目,1,1) = '5' then '人员费用-福利费'
            when substr(a.成本科目,1,1) = '6' then '维修费用'
            when substr(a.成本科目,1,1) = '7' then '协力费'
            when substr(a.成本科目,1,1) = '8' then '一般性厂务费用'
            when substr(a.成本科目,1,1) = '9' then '物料新品'
       end caccount_type,
       sum(b.month_cost) month_cost,
       sum(rd_amount) rd_amount,
       sum(rd_amount)/sum(b.month_cost) ratio_rd_amount,
       sum(rd_amount*coalesce(ratio_adjust,1)) rd_amount_adjust,
       sum(rd_amount*coalesce(ratio_adjust,1))/sum(b.month_cost) ratio_rd_amount_adjust,
       sum(rd_amount*coalesce(ratio_adjust,1))/sum(b.month_cost)*0.3 ratio_rd_amount_adjust_30percent
from orig_product_cost a left join map_project_product_account b
  on substr(a.成本中心,1,instr(a.成本中心,'-')-1)=b.cost_center_code 
 and substr(a.产副品代码,1,instr(a.产副品代码,'-')-1)=b.product_code 
 and a.辅助核算对象=b.ba_object_sub_1 
 and substr(a.成本科目,1,instr(a.成本科目,'-')-1)=b.cost_account_code
group by 1
    """)


# 根据 0628 测试文档 修改
def process_stat_voucher_balance():
    exec_command("drop table if exists stat_voucher_balance")
    exec_command("""
create table stat_voucher_balance as 
select ( select 凭证号码 from voucher_entry b where b."index" = a.voucher_selected ) voucher_no, 
       voucher_selected, cost_account_name, cost_center_name, cost_account_name,amount_selected, rd_amount, 
       ( select sum(b.rd_amount*coalesce(b.ratio_adjust,1)) from map_project_product_account b where b.cost_center_code=a.cost_center_code and b.cost_account_code=a.cost_account_code ) rd_amount_adjusted,
       null balance,
       null balance_ratio
from map_ccenter_caccount_svoucher a
where a.voucher_selected is not null 
  and a.rd_amount is not null 
  and a.voucher_type <> 'TG-BZD'
    """)
    exec_command("""
update stat_voucher_balance 
   set balance = amount_selected - coalesce(rd_amount,0),
       balance_ratio = (amount_selected - coalesce(rd_amount,0)) / amount_selected    
    """)


