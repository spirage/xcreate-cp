ssh com "rm -rf ~/pkg/*"
ssh com "mkdir ~/pkg/src"
rsync -av  --include '.py' --exclude '__pycache__' ../core/ com:~/pkg/src/core/
rsync -av  --include '.py' --exclude '__pycache__' ../service/ com:~/pkg/src/service/
rsync -av  --include '.py' --exclude '__pycache__' ../server/ com:~/pkg/src/server/
scp ../xcpserver.py com:~/pkg/src/ 
scp ../requirements.txt com:~/pkg/
# scp docker/sqlite-autoconf-3460000.tar.gz com:~/pkg/
scp docker/Df-pkg com:~/pkg/Dockerfile
scp docker/dc-pkg.yml com:~/pkg/docker-compose.yml
scp docker/package.sh com:~/pkg/src/
ssh com "cd ~/pkg/ && docker-compose down --rmi local && docker-compose up"
rm -rf ./xcpserver
scp com:~/pkg/src/dist/* ./
