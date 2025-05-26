import utilities
import configparser
import os
import logging
import sackExceptions 
import time
from ultralytics import YOLO
import cv2

config_path = os.path.join(os.getcwd() ,"config.ini")
config =  configparser.ConfigParser()
config.read(config_path)
logger = logging.getLogger(__name__)

def countSacks(objectsCoordinates, uncrossedLineSacks, crossedLineSacks, direction, point1, point2):
    try:
        for id in objectsCoordinates.keys():
            if utilities.point_position(point1, point2, objectsCoordinates.get(id)) == direction:
                if id in crossedLineSacks:
                    crossedLineSacks.remove(id)
                uncrossedLineSacks.append(id)
            else:
                if id in uncrossedLineSacks:
                    uncrossedLineSacks.remove(id)
                    crossedLineSacks.append(id)
                # crossedLineSacks.append(id)
    except Exception as e:
        logger.error(f"Error in countSacks: {e}")
        return None

def sackBagCount(cameraId, bayNo, rtsp, direction, frameWidth, frameHeight,modelName, roi=None):
    try:
        if rtsp:
            cap = utilities.VideoCaptureBuffer(rtsp)
        else:
            raise sackExceptions(code = "SC-001", message = "RTSP stream not provided")
        model = YOLO(modelName)
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
                countSacks(objectCoordinates,)
            
            
    except sackExceptions as e:
        logger.error(e)
        return None
    except Exception as e:
        logger.error(f"Error in sackBagCount: {e}")
        return None