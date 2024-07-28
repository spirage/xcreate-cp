VER="1.9.7"
ENV="test"
PORT="9980"
DATE=$(date +'%Y%m%d')
DBFILE="pub-his/xcp.db-${ENV}-${DATE}"
APFILE="pub-his/xcpserver-${ENV}-${DATE}"

source ./publish.sh
