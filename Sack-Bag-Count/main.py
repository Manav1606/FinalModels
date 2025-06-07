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
  
table = '''create table IF NOT EXISTS sackBag_Analytics 
        (id INTEGER  primary key AUTOINCREMENT, companyCode varchar(40),storeCode VARCHAR(50),bayCode VARCHAR(50),
        loadingCount int(10), unLoadingCount int(10), no_of_count int(10), vehicleNumber varchar(20), isCountIncorrect TINYINT,
        firstFrameFilepath varchar(50), lastFrameFilepath varchar(50), countingStartTime timestamp, countingEndTime timestamp,
            currentTime timeStamp Default current_timestamp)'''

ftpInfo =  config["FTP"]
sackAnalyticsUrl =  config["URLS"]["sackAnalytics"]

def countSackBags(bayDetails):
    try:
        stopEvent = threading.Event()
        bayNo = bayDetails.get("bayNo")
        rtsp = "C:/Users/manav/Downloads/heatMap/Sack-Bag-Count/sack18.mp4"
        direction = "left"
        frameWidth = 960
        frameHeight = 640
        modelName = "best.pt"

        loi = [(437.5, 282.4), (797.5, 417.4)]
        roi = [
            {"x": 452.5, "y": 218.4},
            {"x": 313.5, "y": 465.4},
            {"x": 726.5, "y": 575.4},
            {"x": 814.5, "y": 253.4},
            {"x": 452.5, "y": 218.4},
        ]
        
        t = threading.Thread(
            target=sackBagCount.sackBagCount,
            args=(bayDetails, rtsp, direction, frameWidth, frameHeight, modelName, stopEvent, ftpInfo, sackAnalyticsUrl),
            kwargs={"loi": loi, "roi": roi, "client": client, "table" :table}
        )
        thr[bayNo] = t
        stopEvents[bayNo] = stopEvent
        logger.info(f"Starting thread for bay {bayNo}")
        t.start()
        client.publish("bay/sack/status", json.dumps({"bayNo": bayNo, "status": "started"}))

    except Exception as e:
        logger.error(f"Error in countSackBags: {e}")

def close(bayNo):
    try:
        thread = thr.get(bayNo)
        stop_event = stopEvents.get(bayNo)

        if thread and thread.is_alive():
            if stop_event:
                stop_event.set()
            thread.join(timeout=5)
            logger.info(f"Thread for bay {bayNo} has been closed.")
            client.publish("bay/sack/status", json.dumps({"bayNo": bayNo, "status": "stopped"}))

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
                if bayNo not in thr:
                    countSackBags(bayDetail)
                else:
                    client.publish("bay/sack/status", json.dumps({"bayNo": bayNo, "status": "already running"}))

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
                if bayNo in thr:
                    close(bayNo)
                else:
                    client.publish("bay/sack/status", json.dumps({"bayNo": bayNo, "status": "not running"}))

                # Clean up the stop entry
                data["stop"] = [entry for entry in data.get("stop", []) if entry.get("bayNo") != bayNo]
                utilities.saveDataInJson(filepath, data)

            time.sleep(1)

        except Exception as e:
            logger.error(f"Error in stopCounting: {e}")
            time.sleep(1)

# === MAIN EXECUTION ===
if __name__ == "__main__":
    client = utilities.MQTTClient(client_id="publishers", on_message=on_message)
    client.connect()
    client.loop_start()

    threading.Thread(target=startCounting, daemon=True).start()
    threading.Thread(target=stopCounting, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down MQTT client...")
        client.loop_stop()
        client.disconnect()
