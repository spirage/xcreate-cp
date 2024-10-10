#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "参数异常！"
    echo "使用方法: sudo ./imp_support_data 远程支持回复数据文件"
    exit 1
fi

echo "=============================================================================================="
echo "* 开始导入远程支持回复数据                                                                       *"
echo "=============================================================================================="
echo

if docker ps -a | grep "xcp-prod" > /dev/null; then
   FILE="$1"
   DIRECTORY="${FILE}-extract"
   if [ -d "$DIRECTORY" ]; then
     rm -rf "$DIRECTORY"
   fi
   if [ -e "$FILE" ]; then
     tar -zxvf "$FILE" -C "$DIRECTORY"
     if docker ps | grep "xcp-prod" > /dev/null; then
        docker stop xcp-prod
     fi
     docker cp "${DIRECTORY}/*" xcp-prod:/app/
     rm -rf "$DIRECTORY"
     docker start xcp-prod
     echo "完成导入远程支持回复数据"
   else
     echo "文件 ${FILE} 不存在"
   fi
else
   echo "未检测到运行中的服务，请确保服务已安装"
fi
