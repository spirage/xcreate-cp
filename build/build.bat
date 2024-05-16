
pyinstaller -F ..\xcpserver.py
pyarmor cfg nts=http://worldtimeapi.org/api
pyarmor gen -O obfdist -e 2025-01-01 --pack dist\xcpserver.exe ..\xcpserver.py
