./build.sh
ssh xcp "cd /tmp/_MYEA1i619/ && rm -rf ./* && mkdir app && docker cp xcp-test:/app/xcp.db ./app/"
ENV="test"
DATE=$(date +'%Y%m%d')
DBFILE="pub-his/xcp.db-${ENV}-${DATE}"
APFILE="pub-his/xcpserver-${ENV}-${DATE}"
scp xcp:/tmp/_MYEA1i619/app/xcp.db-bak0704 "$DBFILE"
scp xcpserver xcp:/tmp/_MYEA1i619/app/
mv xcpserver "$APFILE"
scp docker/Df-pub xcp:/tmp/_MYEA1i619/Dockerfile
scp docker/supervisord.conf xcp:/tmp/_MYEA1i619/
ssh xcp "cd /tmp/_MYEA1i619/ && docker stop xcp-test && docker rm xcp-test && docker rmi xcpserver-test && docker build -t xcpserver-test . && docker run -d -p9980:7980 --name xcp-test xcpserver-test && rm -rf ./*"
