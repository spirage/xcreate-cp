docker stop xcp-prod
docker cp xcp-prod:/app/xcp.db ./
sqlite3 xcp.db <<EOF
alter table mat_track_detail add column project_code text;
update mat_track_detail as a
    set project_code=(select 项目编码 from para_project b where b.项目序号=a.project_id)
    where a.project_id is not null;
alter table mat_track_detail drop column project_id;
alter table tpl_para_project drop column 项目序号;
alter table tpl_para_project drop column 匹配重量;

alter table mat_track_detail add column in_storage text;
update mat_track_detail set in_storage = 'Y' where mat_group_no  in (select mat_group_no from para_inventory_transfer);
drop table para_inventory_transfer;
drop table tpl_para_inventory_transfer;
EOF
docker cp xcp.db xcp-prod:/app/
rm -rf xcp.db