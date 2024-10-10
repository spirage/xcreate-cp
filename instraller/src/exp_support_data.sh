#!/bin/bash

echo "=============================================================================================="
echo "* 开始准备远程支持相关数据                                                                       *"
echo "=============================================================================================="
echo

if docker ps -a | grep "xcp-prod" > /dev/null; then
   DIRECTORY="./support_data"
   if [ -d "$DIRECTORY" ]; then
     rm -rf "$DIRECTORY"
   fi
   mkdir "$DIRECTORY"
   TIME=$(date +'%Y%m%d%H%M%S')
   FILE="support_data-${TIME}"
   docker cp xcp-test:/app/xcp.log "$DIRECTORY"
   docker cp xcp-test:/app/xcp.out.log "$DIRECTORY"
   docker cp xcp-test:/app/xcp.err.log "$DIRECTORY"
   docker cp xcp-test:/app/xcp.db "$DIRECTORY"
   tar -zcvf "${FILE}" "$DIRECTORY"
   rm -rf "$DIRECTORY"
   echo "已准备好远程支持相关数据文件 ${FILE}"
else
   echo "未检测到运行中的服务，请确保服务已安装"
fi
