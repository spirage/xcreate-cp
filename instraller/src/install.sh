#!/bin/bash

echo "=============================================================================================="
echo "* 开始安装                                                                                    *"
echo "=============================================================================================="
echo
if docker ps -a | grep "xcp-prod" > /dev/null; then
   echo "服务已存在，无需安装，如需启动服务请运行start命令，如需卸载并重新安装，请先运行uninstall命令"
else
   if [ -d xcp ]; then
     echo "xcp目录已存在，确认xcp目录中重要文件已备份，执行uninstall命令卸载后重新执行install命令"
   else
     tar - zxvf rdea_xcp_installer -C xcp
     cd xcp
     docker load -i xcp-prod.tar
     docker run -d -p7980:7980 --name xcp-prod xcpserver-prod
     rm -rf xcp-prod.tar
     echo "完成安装"
   fi
fi
