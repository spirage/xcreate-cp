docker stop xcp-prod
docker cp xcp-prod:/app/xcp.db ./
sqlite3 xcp.db <<EOF
alter table inventory_summary rename to para_inventory_summary;
alter table vucher_recalculated rename to voucher_recalculated;
EOF
docker cp xcp.db xcp-prod:/app/
rm -rf xcp.db