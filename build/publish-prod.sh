ssh xcp "cd /tmp/_MEIE1i619/ && rm -rf ./* && mkdir app && docker cp xcp-prod:/app/xcp.db ./app/"
scp xcpserver xcp:/tmp/_MEIE1i619/app/
scp docker/Df-pub xcp:/tmp/_MEIE1i619/Dockerfile
scp docker/supervisord.conf xcp:/tmp/_MEIE1i619/
ssh xcp "cd /tmp/_MEIE1i619/ && docker stop xcp-prod && docker rm xcp-prod && docker rmi xcpserver-prod && docker build -t xcpserver-prod . && docker run -d -p7982:7980 --name xcp-prod xcpserver-prod && rm -rf ./*"
