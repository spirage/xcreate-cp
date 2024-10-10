#!/bin/bash

echo
if docker ps | grep "xcp-prod" > /dev/null; then
   echo "ok"
else
   echo "failed, 未检测到运行中的服务"
fi
