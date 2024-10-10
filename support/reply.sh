echo "=============================================================================================="
echo "* 开始生成远程支持回复文件                                                                                    *"
echo "=============================================================================================="
echo
if docker ps -a | grep "xcp-prod" > /dev/null; then
   echo "服务已存在，无需安装，如需卸载并重新安装，请先运行uninstall命令"
else
   tar - zxvf rdea_xcp_installer.tar.gz
   docker load -i xcp-prod.tar
   echo "完成安装"
fi
