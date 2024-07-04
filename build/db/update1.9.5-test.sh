sqlite3 xcp.db <<EOF
alter table inventory_summary rename to para_inventory_summary;
alter table tpl_inventory_summary rename to tpl_para_inventory_summary;
EOF