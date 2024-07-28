ssh xcp "mkdir -p /tmp/_MXEC1p619/ && cd /tmp/_MXEC1p619/ && rm -rf ./* && mkdir app && \
         docker cp xcp-test:/app/xcpserver /tmp/_MXEC1p619/app/ && \
         docker cp xcp-test:/app/xcp.db /tmp/_MXEC1p619/app/ && \
         docker stop xcp-prod && \
         docker cp /tmp/_MXEC1p619/app/xcpserver xcp-prod:/app/ && \
         docker cp /tmp/_MXEC1p619/app/xcp.db xcp-prod:/app/ && \
         docker start xcp-prod
         rm -rf /tmp/_MXEC1p619/*"
