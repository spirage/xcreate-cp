#1 配置 ssh 连接 代码(com)服务器（建议 debian11或 12操作系统）免密登录
#2 代码(com)服务器安装docker、docker-compose和 rsync 等基础环境
#3 配置 ssh 连接 应用(hb-test)服务器免密登录
#4 修改 docker/package.sh中到期时间，如无限使用可以改为 2099年
#5 调整 publish-prod.sh或publish-test.sh中的版本和端口等参数
#6 运行 publish-prod.sh部署 xcp-prod，或者运行 publish-test.sh部署 xcp-test