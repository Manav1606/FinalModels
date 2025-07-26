import dwellTime
import configparser
import os
import json
import threading 
import time
import logging
from datetime import datetime

appFolder = "/home/transline/Documents/storepulse_dwellTime"
config_path = f"{appFolder}/config.ini"
config =  configparser.ConfigParser()
config.read(config_path)

#define logger file
logFolderName = "Dwell_Time_Logger"
if not os.path.exists(logFolderName):
    os.makedirs(logFolderName)
    
log_filename = f"DwellTime_{datetime.now().date()}.log"
log_filepath = f"{appFolder}/{logFolderName}/{log_filename}"
# log_filepath = os.path.join(os.getcwd(),logFolderName, log_filename)
logger = logging.getLogger('dwellTime_logger')
logger.setLevel(logging.INFO)
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
        urls  = config["URLS"]
        imageFolderName = f"{appFolder}/DwellTime"
        operationConfigs = {}
        alertTable = '''create table IF NOT EXISTS DwellTime_Ananlytics 
            (id INTEGER  primary key AUTOINCREMENT,companyCode varchar(40),exhibitionCode VARCHAR(50),boothCode VARCHAR(50) ,
            alertType int(10), filepath varchar(50),mimeType varchar(20), alert_status varchar(20),
            dateandtime timestamp , remark varchar(5000), currentTime timeStamp Default current_timestamp)'''
        
        dwellTable = '''create table IF NOT EXISTS DwellTime
            (id INTEGER  primary key AUTOINCREMENT,companyCode varchar(40),exhibitionCode VARCHAR(50),boothCode VARCHAR(50) ,
            cameraId int(10), data Text , currentTime timeStamp Default current_timestamp)'''
            
        operationConfigs["alertOperations"] = {"table": alertTable, "url": urls.get("alertapi"), "tableName": "DwellTime_Ananlytics"}
        operationConfigs["dwellTime"] = {"table": dwellTable, "url": urls.get("dwelltimeapi"), "tableName": "DwellTime"}
        while True:
            
            if datetime.now().minute % 10 == 0:
                threading.Thread(target =  dwellTime.sendPreviousData, args = (imageFolderName,config["Company-Details"].get("booth_code")),kwargs={'operationConfigs': operationConfigs}).start()
                
            startTime = datetime.strptime(config["Dwell-Time"].get("startTime", "00:01:00"), "%H:%M:%S").time()
            endTime = datetime.strptime(config["Dwell-Time"].get("endTime", "23:59:59"), "%H:%M:%S").time()
            
            if datetime.now().time() >= startTime and datetime.now().time() < endTime:
                thr = []
                
                for cameraInfo in cameras:
                    if cameraInfo:
                        t = threading.Thread(target=dwellTime.detectDwellTime, args=(cameraInfo,frameWidth, frameHeight,startTime, endTime, imageFolderName, operationConfigs))
                        t.start()  
                        thr.append(t)  
                for t in thr:
                    t.join()

                time.sleep(1)
                
    except Exception as e:
        logger.error(f"error in dwellTimeMain  {e}")

if __name__ == "__main__":
    dwellTimeMain()