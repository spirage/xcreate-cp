echo "=============================================================================================="
echo "* 开始发布                                                                                    *"
echo "=============================================================================================="
echo .
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "第一步 生成"
echo .

./build.sh

echo
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "第二步 备份数据库和应用"
echo

ssh xcp "mkdir -p /tmp/_MXEC1p619/ && cd /tmp/_MXEC1p619/ && rm -rf ./* && mkdir app"
ssh xcp "if docker ps -a | grep "xcp-${ENV}" > /dev/null; then docker cp xcp-${ENV}:/app/xcp.db /tmp/_MXEC1p619/app/; fi"
scp xcp:/tmp/_MXEC1p619/app/xcp.db "$DBFILE"
scp ./xcpserver xcp:/tmp/_MXEC1p619/app/
mv ./xcpserver "$APFILE"

echo
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "第三步 升级数据库"
echo
if [ -f "db/update${VER}-${ENV}.sh" ]; then
    scp "db/update${VER}-${ENV}.sh" xcp:/tmp/_MXEC1p619/app/
    ssh xcp "cd /tmp/_MXEC1p619/app/ && ./update${VER}-${ENV}.sh && rm -rf ./update${VER}-${ENV}.sh"
    echo "完成数据库升级"
else
    echo "没有升级文件"
fi


echo
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "第四步 执行发布"
echo

scp docker/Df-pub xcp:/tmp/_MXEC1p619/Dockerfile
scp docker/supervisord.conf xcp:/tmp/_MXEC1p619/
ssh xcp "if docker ps -a | grep "xcp-${ENV}" > /dev/null; then docker stop xcp-${ENV}; docker rm xcp-${ENV}; docker rmi xcpserver-${ENV}; fi"
ssh xcp "cd /tmp/_MXEC1p619/ && \
         docker build -t xcpserver-${ENV} . && \
         docker run -d -p${PORT}:7980 --name xcp-${ENV} xcpserver-${ENV} && rm -rf ./*"
echo
echo "=============================================================================================="
echo "* 完成发布                                                                                    *"
echo "=============================================================================================="
echo