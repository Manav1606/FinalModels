import sackBagCount
import logging
import os
import threading
import utilities
import configparser
import queue
import time
import json
from pathlib import Path
import cv2
import traceback

# Load config
config = configparser.ConfigParser()
config_path = os.path.join(os.getcwd(), "config.ini")
if os.path.exists(config_path):
    config.read(config_path)

# Logger setup
log_filename = "sackBagCount.log"
log_filepath = os.path.join(os.getcwd(), log_filename)
logger = logging.getLogger("sackBag_logger")
logger.setLevel(logging.INFO)
if not logger.handlers:
    file_mode = "a" if os.path.exists(log_filepath) else "w"
    file_handler = logging.FileHandler(log_filepath, mode=file_mode)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# MQTT Message Handler
def on_message(client, userdata, message):
    queue_data.put(message.payload.decode("utf-8"))

queue_data = queue.Queue()

thr = {}
stopEvents = {}
  
table = '''CREATE TABLE IF NOT EXISTS sackBag_Analytics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    companyCode VARCHAR(40),
    storeCode VARCHAR(50),
    bayCode VARCHAR(50),
    loadingCount INTEGER,
    unLoadingCount INTEGER,
    noOfCounts INTEGER,
    vehicleNumber VARCHAR(20),
    isCountIncorrect TINYINT,
    firstFrameFilepath VARCHAR(50),
    lastFrameFilepath VARCHAR(50),
    countingStartTime TIMESTAMP,
    countingEndTime TIMESTAMP,
    isAlertTriggerd BOOLEAN,
    alertReason VARCHAR(40),
    currentTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''
            

ftpInfo =  config["FTP"]
sackAnalyticsUrl =  config["URLS"]["sackAnalytics"]
bayInfoUrl = config["URLS"]["getBayDetails"]
imageFolderName = f"sack_data/sack_bag_frames/"

def sendPreviousDataOnCloud(ftpInfo, ftpFolder, imageFolderName, table = None, url = None):
    try:
        ftp = utilities.setupFtp(ftpInfo.get("username"), ftpInfo.get("password"), ftpInfo.get("host"), int(ftpInfo.get("port")))
        if not ftp:
            logger.error("FTP connection failed")
            return
        
        if os.path.exists(imageFolderName):
            for compSubfolder in os.listdir(imageFolderName):
                compSubfolderPath = os.path.join(imageFolderName, compSubfolder)
                for storeSubFolder in os.listdir(compSubfolderPath):
                    storeSubfolderPath = os.path.join(compSubfolderPath, storeSubFolder)
                    for baySubFolder in os.listdir(storeSubfolderPath):
                        baySubfolderPath = os.path.join(storeSubfolderPath, baySubFolder)
                        images = [file for file in os.listdir(baySubfolderPath) if file.endswith(('.jpg'))]
                        ftp.ftp_mkdir_recursive(os.path.join(ftpFolder, compSubfolder, storeSubFolder, baySubFolder))
                        for image in images:
                            ftpImageLocation  = f"{ftpFolder}/{compSubfolder}/{storeSubFolder}/{baySubFolder}/{image}"
                            frame  = cv2.imread(os.path.join(baySubfolderPath, image))
                            ftpres = utilities.uploadFileOnFtp(ftp, frame, ftpImageLocation)
                            if ftpres:
                                os.remove(os.path.join(baySubfolderPath, image))
                            else:
                                if ftp is not None:
                                    ftp.close()
                                ftp.connect()
                        if not os.listdir(baySubfolderPath):
                            os.rmdir(baySubfolderPath)
                    if not os.listdir(storeSubfolderPath):
                        os.rmdir(storeSubfolderPath)
                if not os.listdir(compSubfolderPath):
                    os.rmdir(compSubfolderPath)
        
        if ftp is not None:
            ftp.close()    
            
        try:
            conn = utilities.setupDB(table)
        except Exception as e:
            logger.error(f"Error in setupDB: {e}")
            
        if conn:
            cusor = conn.cursor
            cusor.execute('''select * from sackBag_Analytics''')
            allPreviousData = cusor.fetchall()
            if allPreviousData:
                for data in allPreviousData:
                    apiData = {
                        "company_code": data[1],
                        "store_code": data[2],
                        "bay_code": data[3],
                        "loading_count": data[4],
                        "unloading_count": data[5],               
                        "no_of_counts": data[6],
                        "vehicle_number": data[7],
                        "is_count_incorrect": data[8],
                        "first_frame": data[9],
                        "last_frame": data[10],
                        "counting_start_time": data[11],
                        "counting_end_time": data[12],
                        "is_alert_triggered": data[13],
                        "alert_reason": data[14]
                    }
                    res = utilities.sendRequest(url, apiData)
                    if res.get("status") ==  200:
                        cusor.execute('''delete from sackBag_Analytics where id = ?''', (data[0],))
                        conn.commit()
            conn.close()
                
    except Exception as e:
        logger.error("Error in sendPreviousDataOnCloud:\n" + traceback.format_exc())

    return


def countSackBags(bayDetails):
    try:
        stopEvent = threading.Event()
        bayNo = bayDetails.get("bayNo")
        frameWidth = 960
        frameHeight = 640
        urlWithParam =  f"{bayInfoUrl}/{bayNo}"
        res = utilities.sendRequest(urlWithParam, method= "GET")
        if res.get("status") != 200:
            client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "Error Not started Yet", "statusCode" : 400}))
        else:
            data = res.get("data")
            rtsp = data.get("rtsp_url")
            direction = data.get("loading_direction")
            modelName = "sackbag_75epochs_270625.pt"

            loi = data.get("loi")
            roi = data.get("roi")
            
            t = threading.Thread(
                target=sackBagCount.sackBagCount,
                args=(bayDetails, rtsp, direction, frameWidth, frameHeight, modelName, stopEvent, ftpInfo, sackAnalyticsUrl),
                kwargs={"loi": loi, "roi": roi, "client": client, "table" :table}
            )
            thr[bayNo] = t
            stopEvents[bayNo] = stopEvent
            logger.info(f"Starting thread for bay {bayNo}")
            t.start()
            # client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "started", "statusCode" : 200}))

    except Exception as e:
        logger.error(f"Error in countSackBags: {e}")
        client.publish("bay/sack/status", json.dumps({"bayNo": bayNo, "status": "Error Not started Yet"}))

def close(bayNo):
    try:
        thread = thr.get(bayNo)
        stop_event = stopEvents.get(bayNo)

        if thread and thread.is_alive():
            if stop_event:
                stop_event.set()
            thread.join(timeout=5)
            logger.info(f"Thread for bay {bayNo} has been closed.")
            client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "stopped", "statusCode": 200}))

        thr.pop(bayNo, None)
        stopEvents.pop(bayNo, None)

    except Exception as e:
        logger.error(f"Error in close: {e}")

def read_json_file(filepath):
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        with open(filepath, "w") as f:
            json.dump({"start": [], "stop": []}, f, indent=4)
    with open(filepath, "r") as f:
        return json.load(f)

def startCounting():
    filepath = os.path.join(os.getcwd(), "mqtt.json")
    while True:
        try:
            data = read_json_file(filepath)
            for bayDetail in data.get("start", []):
                bayNo = bayDetail.get("bayNo")
                if bayDetail.get("isCheck") == 1 and bayNo in thr:
                    client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "already running", "statusCode": 201}))
                elif bayDetail.get("isCheck") == 1 and bayNo not in thr:
                    client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "Not running", "statusCode": 202}))
                elif bayNo not in thr and bayDetail.get("isCheck") == 0:
                    countSackBags(bayDetail)
                else:
                    client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "already running", "statusCode": 201}))

                data["start"] = [entry for entry in data.get("start", []) if entry.get("bayNo") != bayNo]
                utilities.saveDataInJson(filepath, data)

            time.sleep(1)

        except Exception as e:
            logger.error(f"Error in startCounting: {e}")
            time.sleep(1)

def stopCounting():
    filepath = os.path.join(os.getcwd(), "mqtt.json")
    while True:
        try:
            data = read_json_file(filepath)
            for bayDetail in data.get("stop", []):
                bayNo = bayDetail.get("bayNo")
                if bayDetail.get("isCheck") == 1 and bayNo not in thr:
                    client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "not running", "statusCode": 202})) 
                elif bayDetail.get("isCheck") == 1 and bayNo in thr:
                    client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "already running", "statusCode": 201}))  
                elif bayNo in thr and bayDetail.get("isCheck") == 0:
                    close(bayNo)
                else:
                    client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "not running", "statusCode": 202}))

                # Clean up the stop entry
                data["stop"] = [entry for entry in data.get("stop", []) if entry.get("bayNo") != bayNo]
                utilities.saveDataInJson(filepath, data)

            time.sleep(1)

        except Exception as e:
            logger.error(f"Error in stopCounting: {e}")
            time.sleep(1)

# === MAIN EXECUTION ===
if __name__ == "__main__":
    mqtt = config["MQTT"]
    client = utilities.MQTTClient(client_id=mqtt["clientId"], broker = mqtt["broker"], on_message=on_message, transport=mqtt["transport"])
    client.connect()
    client.loop_start()

    threading.Thread(target=startCounting, daemon=True).start()
    threading.Thread(target=stopCounting, daemon=True).start()
    syncTime = int(time.time())
    
    try:
        while True:
            try:
                if int(time.time()) - syncTime > 600:
                    sendPreviousDataOnCloud(ftpInfo, ftpInfo.get("ftp_location"), imageFolderName, table=table, url=sackAnalyticsUrl)
                    syncTime = int(time.time())
            except Exception as e:
                continue
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down MQTT client...")
        client.loop_stop()
        client.disconnect()
