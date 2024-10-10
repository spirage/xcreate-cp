#!/bin/bash

echo "=============================================================================================="
echo "* 开始卸载                                                                                    *"
echo "=============================================================================================="
echo
if docker ps -a | grep "xcp-prod" > /dev/null; then
   docker stop xcp-prod
   docker rm xcp-prod
   docker rmi xcpserver-prod
   echo "完成服务卸载"
elif docker images | grep "xcpserver-prod" > /dev/null; then
   docker rmi xcpserver-prod;
   echo "完成服务卸载"
else
   echo "服务不存在，无需卸载"
fi

if [ -d xcp ]; then
   rm -rf xcp
   echo "完成文件卸载"
else
   echo "文件不存在，无需卸载"
fi

