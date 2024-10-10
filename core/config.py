# -*- coding: UTF-8 -*-
import os
from core.log import *
from dotenv import dotenv_values, load_dotenv

env_name = "test"
env_file = ".env." + env_name
if not os.path.exists(env_file):
    env_file = "../" + env_file
if not os.path.exists(env_file):
    logger.error("配置文件 " + env_file + " 文件不存在")
load_dotenv(env_file)
config = dotenv_values(env_file)
logger.info("===========================================================================")
logger.info("环境参数配置：")
logger.info("    软件版本：     " + str(config.get('CORE_VER')))
logger.info("    部署环境：     " + str(config.get('CORE_ENV')))
logger.info("    监听地址：     " + str(config.get('CORE_HOST')))
logger.info("    监听端口：     " + str(config.get('CORE_PORT')))
logger.info("    容器地址：     " + str(config.get('CNTR_HOST')))
logger.info("    容器端口：     " + str(config.get('CNTR_PORT')))
logger.info("===========================================================================")

