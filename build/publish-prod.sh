VER="1.9.13"
ENV="prod"
PORT="7980"
DATE=$(date +'%Y%m%d')
DBFILE="pub-his/xcp.db-${ENV}-${DATE}"
APFILE="pub-his/xcpserver-${ENV}-${DATE}"

source ./publish.sh
