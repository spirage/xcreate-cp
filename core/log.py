# -*- coding: UTF-8 -*-

import logging
import sys


def get_logger():
    file_handler = logging.FileHandler('xcp.log', mode='a', encoding="utf8")
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s[%(module)s.%(lineno)d]:	%(message)s'
    ))
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(
        '[%(asctime)s %(levelname)s] %(message)s',
        datefmt="%Y/%m/%d %H:%M:%S"
    ))
    console_handler.setLevel(logging.INFO)

    logging.basicConfig(
        level=min(logging.INFO, logging.DEBUG),
        handlers=[file_handler, console_handler],
    )

    return logging.getLogger('XCP')


logger = get_logger()
