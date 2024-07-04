docker stop xcp-prod
docker cp xcp-prod:/app/xcp.db ./
sqlite3 xcp.db <<EOF
--alter table inventory_summary rename to para_inventory_summary;
alter table vucher_recalculated rename to voucher_recalculated;
create table sys_config(id integer, name text, key text, value text, update_time datetime);
insert into sys_config
values (1, '会计主体', 'sys.acc_entity', '集团公司', '20240626 10:00:32'),
       (2, '会计期', 'sys.acc_period', '202312', '20240626 10:00:33');
EOF
docker cp xcp.db xcp-prod:/app/
rm -rf xcp.db