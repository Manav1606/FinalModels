import dwellTime
import configparser
import os
import json
import threading 
import time
import logging
from datetime import datetime

config_path = os.path.join(os.getcwd(),"Dwell_Time", "config.ini")
config =  configparser.ConfigParser()
config.read(config_path)

#define logger file
log_filename = f"DwellTime_{datetime.now().date()}.log"
log_filepath = os.path.join(os.getcwd(), log_filename)
logger = logging.getLogger('dwellTime_logger')
logger.setLevel(logging.ERROR)
if not logger.handlers:
    file_mode = 'a' if os.path.exists(log_filepath) else 'w'
    file_handler = logging.FileHandler(log_filepath, mode=file_mode)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

def dwellTimeMain():
    try:
        cameras = json.loads(config["Dwell-Time"]["cameras_info"])
        frameWidth, frameHeight = int(config["Dwell-Time"]["frameWidth"]), int(config["Dwell-Time"]["frameHeight"])
        while True:
            thr = []

            for cameraInfo in cameras:
                if cameraInfo:
                    t = threading.Thread(target=dwellTime.detectDwellTime, args=(cameraInfo,frameWidth, frameHeight))
                    t.start()  
                    thr.append(t)  
            for t in thr:
                t.join()

            time.sleep(1)
    except Exception as e:
        logger.error(f"error in dwellTimeMain  {e}")

if __name__ == "__main__":
    dwellTimeMain()