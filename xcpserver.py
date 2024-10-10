# -*- coding: UTF-8 -*-
from signal import signal, SIGINT
import uvicorn
from server.app import *


def ignore_signal(sig, frame):
    pass


if __name__ == '__main__':
    try:
        host = config.get('CORE_HOST')
        port = config.get('CORE_PORT')
        logger.info("xcp server start listening on http://" + str(host) + ":" + str(port))
        logger.info("xcp swagger document url is http://" + str(host) + ":" + str(port) + "/doc")
        signal(SIGINT, ignore_signal)
        uvicorn.run(app, host=host, port=port, log_level=logging.CRITICAL, server_header=None)
    except Exception as e:
        logger.error(str(e))
