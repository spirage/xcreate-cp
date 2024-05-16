rm -rf ./build/
rm -rf ./dist/
rm -rf ./xcpserver.spec
pyinstaller -F ../xcpserver.py
pyarmor cfg nts=http://worldtimeapi.org/api
pyarmor gen --platform linux.x86_64 -O obfdist -e 2025-01-01 --pack dist/xcpserver ../xcpserver.py
cp dist/xcpserver docker/app/xcpserver
cd docker
docker-compose down --rmi local
docker-compose up -d

