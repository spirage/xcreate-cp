pyinstaller -F xcpserver.py && \
pyarmor cfg nts=http://worldtimeapi.org/api && \
pyarmor gen --platform linux.x86_64 -O obfdist -e 2025-01-01 --pack dist/xcpserver xcpserver.py
