# -*- coding: UTF-8 -*-
from signal import signal, SIGINT
import uvicorn
from server.app import *


def ignore_signal(sig, frame):
    pass


if __name__ == '__main__':
    try:
        logger.info("xcp server start listening on http://localhost:7980")
        signal(SIGINT, ignore_signal)
        uvicorn.run(app, host='0.0.0.0', port=7980, log_level=logging.CRITICAL, server_header=None)
    except Exception as e:
        logger.error(str(e))
