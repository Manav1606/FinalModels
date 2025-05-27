import utilities
import configparser
import os
import logging
import sackExceptions 
import time
from ultralytics import YOLO
import cv2
import json

config_path = os.path.join(os.getcwd() ,"config.ini")
config =  configparser.ConfigParser()
config.read(config_path)
logger = logging.getLogger(__name__)

def countSacks(objectsCoordinates, uncrossedLineSacks, crossedLineSacks, direction, point1, point2):
    try:
        for id in objectsCoordinates.keys():
            if utilities.point_position(point1, point2, objectsCoordinates.get(id)) != direction:
                if id in uncrossedLineSacks["loading"]:
                    crossedLineSacks["loading"].append(id)
                    uncrossedLineSacks["loading"].remove(id)
                else:
                    uncrossedLineSacks["unLoading"].append(id)
                    if id in crossedLineSacks["unLoading"]:
                        crossedLineSacks["unLoading"].remove(id)
            else:
                if id in uncrossedLineSacks["unLoading"]:
                    uncrossedLineSacks["unLoading"].remove(id)
                    crossedLineSacks["unLoading"].append(id)
                else:
                    uncrossedLineSacks["loading"].append(id)
                    if id in crossedLineSacks["loading"]:
                        crossedLineSacks["loading"].remove(id)  
                # crossedLineSacks.append(id)
    except Exception as e:
        logger.error(f"Error in countSacks: {e}")
        return None

def sackBagCount(cameraId, bayNo, rtsp, direction, frameWidth, frameHeight,modelName,companyCode, storeCode,countLimit = 0, loi=None, roi=None):
    try:
        if rtsp:
            cap = utilities.VideoCaptureBuffer(rtsp)
        else:
            raise sackExceptions(code = "SC-001", message = "RTSP stream not provided")
        
        if modelName:
            model = YOLO(modelName)
        else:
            raise sackExceptions(code = "SC-002", message = "Model name not provided")
        
        if not os.path.exists("sack_data"):
            os.makedirs("sack_data")
            
        fileName = f"sack_data/sack_bag_count_{companyCode}_{storeCode}_{cameraId}_{bayNo}.json"
            
        uncrossedLineSacks = {"loading": [], "unLoading": []}
        crossedLineSacks = {"loading": [], "unLoading": []}
        
        while True:
            ret, frame = cap.read()
            if not ret:
                cap.release()
                cap = utilities.VideoCaptureBuffer(rtsp)
                time.sleep(1)
                continue
            frame = cv2.resize(frame, (frameWidth, frameHeight))
            results =  model.track(frame,imgsz = 640, conf=0.2,persist=True, iou = 0.4, tracker = "bytetrack.yaml", verbose =False)
            objectCoordinates = utilities.fetchObject(results, objects=[1], roi = roi)
            if objectCoordinates is not None:
                if loi is not None:
                    countSacks(objectCoordinates,uncrossedLineSacks, crossedLineSacks, direction, loi[0], loi[1])
                    # publish data to MQTT broker
                    data  = {
                        "unloadingSacks": len(crossedLineSacks["unLoading"]),
                        "loadingSacks": len(crossedLineSacks["loading"]),
                    }
                    utilities.saveDataInJson(fileName, data)
                else:
                    raise sackExceptions(code = "SC-003", message = "Line of Interest (loi) not provided")
                # post alert if countLimit is reached in api
            if cv2.waitKey(1) & 0xFF == ord('q'):  # Press 'q' to exit
                break
            
            
    except sackExceptions as e:
        logger.error(e)
        return None
    except Exception as e:
        logger.error(f"Error in sackBagCount: {e}")
        return None