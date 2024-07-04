echo "=============================================================================================="
echo "* 开始发布                                                                                    *"
echo "=============================================================================================="
echo .
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "第一步 生成"
echo .

./build.sh

VER="1.9.5"
ENV="test"
DATE=$(date +'%Y%m%d')
DBFILE="pub-his/xcp.db-${ENV}-${DATE}"
APFILE="pub-his/xcpserver-${ENV}-${DATE}"

echo .
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "第二步 备份应用和数据库"
echo .

ssh xcp "mkdir -p /tmp/_MXEC1p619/ && cd /tmp/_MXEC1p619/ && rm -rf ./* && mkdir app && docker cp xcp-${ENV}:/app/xcp.db ./app/"
scp xcp:/tmp/_MXEC1p619/app/xcp.db "$DBFILE"
scp xcpserver xcp:/tmp/_MXEC1p619/app/
mv xcpserver "$APFILE"

echo .
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "第三步 升级数据库"
echo .
if [ -f "db/update${VER}-${ENV}.sh" ]; then
    scp "db/update${VER}-${ENV}.sh" xcp:/tmp/_MXEC1p619/app/
    ssh xcp "cd /tmp/_MXEC1p619/app/ && ./update${VER}-${ENV}.sh"
    echo "完成数据库升级"
else
    echo "没有升级文件"
fi


echo .
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "第四步 执行发布"
echo .

scp docker/Df-pub xcp:/tmp/_MXEC1p619/Dockerfile
scp docker/supervisord.conf xcp:/tmp/_MXEC1p619/
ssh xcp "cd /tmp/_MXEC1p619/ && \
         docker stop xcp-${ENV} && \
         docker rm xcp-${ENV} && \
         docker rmi xcpserver-${ENV} && \
         docker build -t xcpserver-${ENV} . && \
         docker run -d -p9980:7980 --name xcp-${ENV} xcpserver-${ENV} && rm -rf ./*"
