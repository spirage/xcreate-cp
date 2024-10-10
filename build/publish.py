# -*- coding: UTF-8 -*-
from datetime import datetime
from core.config import *
import subprocess


if __name__ == '__main__':
    try:
        date_str = datetime.now().strftime("%Y%m%d")
        os.environ['DBFILE'] = 'pub-his/xcp.db-' + str(config.get('CORE_ENV')) + '-' + date_str
        os.environ['APFILE'] = 'pub-his/xcpserver-' + str(config.get('CORE_ENV')) + '-' + date_str

        script = "./publish.sh"
        process = subprocess.Popen(script, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            print(line, end='')
        process.wait()
        if process.returncode != 0:
            print(f"Shell script execution failed with return code {process.returncode}")
        else:
            print("Shell script executed successfully")

    except Exception as e:
        logger.error(str(e))
