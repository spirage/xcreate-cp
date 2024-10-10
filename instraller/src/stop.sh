#!/bin/bash

echo
if docker ps | grep "xcp-prod" > /dev/null; then
   docker stop xcp-prod
   echo "已停止"
else
   echo "服务未运行，无需停止"
fi
