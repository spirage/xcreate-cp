#!/bin/bash

echo
if docker ps | grep "xcp-prod" > /dev/null; then
   echo "服务运行中，无需启动"
else
   docker start xcp-prod
   echo "已启动"
fi
