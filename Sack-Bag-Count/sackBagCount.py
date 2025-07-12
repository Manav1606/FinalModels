import utilities
import configparser
import os
import logging
from sackExceptions import sackExceptions 
import time
from ultralytics import YOLO
import cv2
import json
from datetime import datetime
import traceback
import numpy as np
import main
import threading

config_path = os.path.join(os.getcwd() ,"Sack-Bag-Count","config.ini")
config =  configparser.ConfigParser()
config.read(config_path)
logger = logging.getLogger("sackBag_logger")


def countSacks(objectsCoordinates, uncrossedLineSacks, crossedLineSacks, direction, point1, point2):
    try:
        for id in objectsCoordinates.keys():
            if utilities.point_position(point1, point2, objectsCoordinates.get(id)) != direction:
                if id in uncrossedLineSacks["loading"]:
                    crossedLineSacks["loading"].append(id)
                    uncrossedLineSacks["loading"].remove(id)
                else:
                    if id not in uncrossedLineSacks["unLoading"]:
                        uncrossedLineSacks["unLoading"].append(id)
                    if id in crossedLineSacks["unLoading"]:
                        crossedLineSacks["unLoading"].remove(id)
            else:
                if id in uncrossedLineSacks["unLoading"]:
                    uncrossedLineSacks["unLoading"].remove(id)
                    crossedLineSacks["unLoading"].append(id)
                else:
                    if id not in uncrossedLineSacks["loading"]:
                        uncrossedLineSacks["loading"].append(id)
                    if id in crossedLineSacks["loading"]:
                        crossedLineSacks["loading"].remove(id)  
                # crossedLineSacks.append(id)
    except Exception as e:
        logger.error(f"Error in countSacks: {e}")
        return None
    
def saveDataInLocalDB(conn, data, startTime, isClosed, triggerAlert = 0):
    try:
        if not conn:
            logger.error("Connection is None")
            return
        cursor = conn.cursor
        cursor.execute('''select id from sackBag_Analytics where companyCode = ? and storeCode = ? and bayCode = ? and countingStartTime = ?''',(data.get("company_code"), data.get("store_code"), data.get("bay_code"), startTime))
        res = cursor.fetchone()
        if res:
            if not triggerAlert:
                cursor.execute('''update sackBag_Analytics set loadingCount = ?, unloadingCount = ?, lastFrameFilepath = ?, countingEndTime = ? where id = ?''',
                            (data.get("loading_count"), data.get("unloading_count"), data.get("last_frame"), startTime, res[0]))
            else:
                cursor.execute('''update sackBag_Analytics set isAlertTriggerd = ?, alertReason = ? where id = ?''',
                            (data.get("is_alert_triggered"), data.get("alert_reason"), res[0]))
        elif isClosed:
            cursor.execute('''insert into sackBag_Analytics (companyCode, storeCode, bayCode, loadingCount, unLoadingCount, lastFrameFilepath, countingEndTime, countingStartTime) values (?, ?, ?, ?, ?, ?, ?, ?)''',
                         (data.get("company_code"), data.get("store_code"), data.get("bay_code"), data.get("loading_count"), data.get("unloading_count"), data.get("last_frame"), data.get("counting_end_time"), startTime))
        elif triggerAlert:
            cursor.execute('''insert into sackBag_Analytics (companyCode, storeCode, bayCode, isAlertTriggerd = ?, alertReason = ?, countingStartTime = ?) values (?, ?, ?, ?, ?, ?)''',
                         (data.get("company_code"), data.get("store_code"), data.get("bay_code"),data.get("is_alert_triggered"), data.get("alert_reason"), startTime))
        else:
            cursor.execute('''insert into sackBag_Analytics (companyCode, storeCode, bayCode, noOfCounts, vehicleNumber, firstFrameFilepath, countingStartTime) values (?, ?, ?, ?, ?, ?, ?)''',
                         (data.get("company_code"), data.get("store_code"), data.get("bay_code"), data.get("no_of_counts"), data.get("vehicle_number"), data.get("first_frame"), startTime))
        conn.commit()
    except Exception as e:
        logger.error(f"Error in saveDataInLocalDB: {e}")
    finally:
        if conn:
            conn.close()
    
def uploadDataOnCloud(
    ftpInfo, folderName, imageName, frame, imageFolderName, 
    bayDetail,  isClosed = False, table = None, 
    loadingCount = 0, unLoadingCount = 0, isCountIncorrect = False,
    url = None, triggerAlert = 0, startTime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    alertReason  = "Count limit exceeded"
    ):
    try:
        fileName = f"{folderName}/{imageName}"
        if frame is not None:
            ftp = None
            try:
                ftp = utilities.setupFtp(ftpInfo.get("username"), ftpInfo.get("password"), ftpInfo.get("host"), int(ftpInfo.get("port")))
            except Exception as e:
                logger.error(f"Error setting up FTP: {e}")
            ftpRes = utilities.uploadFileOnFtp(ftp, frame, fileName)
            if not ftpRes:
                logger.error("Error in uploadFileOnFtp")
                if not os.path.exists(imageFolderName):
                    os.makedirs(imageFolderName)
                imagePath = f"{imageFolderName}/{imageName}"
                success = cv2.imwrite(imagePath, frame)
                if success:
                    logger.error(f"Image saved to {imagePath}")
                else:
                    logger.error(f"Failed to save image to {imagePath}")

            if ftp is not None:
                ftp.close()
        if not isClosed and triggerAlert:
            apiData = {
                        "company_code": bayDetail.get("companyCode"),
                        "store_code": bayDetail.get("storeCode"),
                        "bay_code": bayDetail.get("bayNo"),
                        "counting_start_time": startTime,
                        "is_alert_triggered": True,
                        "alert_reason": alertReason
                    }   
        elif not isClosed:
            apiData = {
                "company_code": bayDetail.get("companyCode"),
                "store_code": bayDetail.get("storeCode"),
                "bay_code": bayDetail.get("bayNo"),
                "no_of_counts": int(bayDetail["counter"]) if str(bayDetail.get("counter", "")).isdigit() else None,
                "vehicle_number": bayDetail.get("vehicleNumber", None),
                "first_frame": f"ftp://ftp.ttpltech.in//{fileName}",
                "counting_start_time": startTime,
            }
        else:
            apiData = {
                "company_code": bayDetail.get("companyCode"),
                "store_code": bayDetail.get("storeCode"),
                "bay_code": bayDetail.get("bayNo"),
                "loading_count": loadingCount,
                "unloading_count": unLoadingCount,
                "is_count_incorrect": isCountIncorrect,
                "last_frame": f"ftp://ftp.ttpltech.in//{fileName}",
                "counting_start_time": startTime,
                "counting_end_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }
        
        res = utilities.sendRequest(url, apiData)
        if res.get("status") != 200:
            try:
                conn = utilities.setupDB(table)
            except Exception as e:
                logger.error(f"Error in setupDB: {e}")
            saveDataInLocalDB(conn, apiData, startTime, isClosed, triggerAlert= triggerAlert)
            
    except Exception as e:
        logger.error(f"Error uploading data on cloud:" + traceback.format_exc())
        return None
    return

def sackBagCount(bayDetails, rtsp, direction, frameWidth, frameHeight,modelName, stopEvent,ftpInfo , sackAnalyticsUrl, loi=None, roi=None, client = None, table = None):
    try:
        logger.info("starting sackBagCount thread")
        
        bayNo = bayDetails.get("bayNo")
        companyCode = bayDetails.get("companyCode")
        storeCode = bayDetails.get("storeCode")
        
        countLimit = ""
        if bayDetails.get("counter") != "":
            countLimit = int(bayDetails.get("counter"))
        startTime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        if not os.path.exists("sack_data"):
            os.makedirs("sack_data")
        
        imageFolderName = f"sack_data/sack_bag_frames/{companyCode}/{storeCode}/{bayNo}"
        if not os.path.exists(imageFolderName):
            os.makedirs(imageFolderName)
        
        ftpFolder = f"{ftpInfo.get('ftp_location')}/{companyCode}/{storeCode}/{bayNo}"
        
        try:
            ftp = utilities.setupFtp(ftpInfo.get("username"), ftpInfo.get("password"), ftpInfo.get("host"), int(ftpInfo.get("port")))
            ftp.ftp_mkdir_recursive(ftpFolder)
        except Exception as e:
            logger.error(f"Error setting up FTP: {e}")
            
        if rtsp:
            cap = cv2.VideoCapture(rtsp)
            while True:
                ret, frame = cap.read()
                if not ret or frame is None:
                    logger.error("Failed to connect to RTSP stream, retrying...")
                    time.sleep(1)
                    cap = cv2.VideoCapture(rtsp)
                    continue
                else:
                    try:
                        imageName = f"first_frame_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
                        threading.Thread(target = uploadDataOnCloud, args = (ftpInfo, ftpFolder, imageName, frame, imageFolderName, 
                            bayDetails), kwargs = {"table": table, "url" : sackAnalyticsUrl,
                            "startTime" : startTime} ).start()
                        
                        # uploadDataOnCloud(
                        #     ftpInfo, ftpFolder, imageName, frame, imageFolderName, 
                        #     bayDetails, table = table, url = sackAnalyticsUrl,
                        #     startTime= startTime
                        # )
                        break
                        
                    except Exception as e:
                        logger.error(f"Error setting up FTP: {e}")
                        return None
                    
        else:
            raise sackExceptions(code = "SC-001", message = "RTSP stream not provided")
        
        if modelName:
            model = YOLO(modelName)
        else:
            raise sackExceptions(code = "SC-002", message = "Model name not provided")
            
        fileName = f"sack_data/sack_bag_count_{companyCode}_{storeCode}_{bayNo}.json"
            
        uncrossedLineSacks = {"loading": [], "unLoading": []}
        crossedLineSacks = {"loading": [], "unLoading": []}
        alertTriggered = 0
        
        # fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        # out = cv2.VideoWriter('sack_bag_detection_count.mp4', fourcc, 25.0, (frameWidth, frameHeight))
        client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "started", "statusCode" : 200}))
        
        while not stopEvent.is_set():
            
            if stopEvent.is_set():
                logger.info("Stopping sackBagCount thread")
                break
            
            ret, frame = cap.read()
            if not ret:
                cap.release()
                print("Reconnecting to RTSP stream...")
                cap = cv2.VideoCapture(rtsp)
                time.sleep(1)
                continue
            frame = cv2.resize(frame, (frameWidth, frameHeight))
            results =  model.track(frame,imgsz = 640, conf=0.2,persist=True, iou = 0.4, tracker = "bytetrack.yaml", verbose =False)
            frame = cv2.line(frame, (int(loi[0][0]),int(loi[0][1]) ), (int(loi[1][0]), int(loi[1][1])), color=(0, 255, 0), thickness=2)
            objectCoordinates = utilities.fetchObject(results, objects=[1], roi = roi)
            if objectCoordinates.get(1) is not None:
                if loi is not None:
                    countSacks(objectCoordinates.get(1),uncrossedLineSacks, crossedLineSacks, direction, loi[0], loi[1])
                    for id in objectCoordinates.get(1).keys():
                        x, y = objectCoordinates.get(1).get(id)
                    # publish data to MQTT broker
                    if not alertTriggered and countLimit != "":
                        countLimit = int(countLimit)
                        if len(crossedLineSacks["unLoading"]) > countLimit or len(crossedLineSacks["loading"]) > countLimit:
                            threading.Thread(target = uploadDataOnCloud, args = (None, None, None, None, None, 
                                bayDetails), kwargs = {"table": table, "url" : sackAnalyticsUrl,
                                "startTime" : startTime, "triggerAlert" : 1} ).start()
                            logger.info("alert Triggered")
                            # uploadDataOnCloud(
                            #     None, None, None, None, None, 
                            #     bayDetails, table = table, url = sackAnalyticsUrl,
                            #     triggerAlert = 1,
                            #     startTime= startTime
                            # )
                            alertTriggered = 1
                        
                    publish = {}
                    data  = {
                        "unloadingSacks": len(crossedLineSacks["unLoading"]),
                        "loadingSacks": len(crossedLineSacks["loading"]),
                    }
                    publish = {
                        "bayNo":bayNo, 
                        "data": data,
                    }
                    if client:
                        # print("publish")
                        client.publish("sack/bag/counter",json.dumps(publish))
                        
                    # utilities.saveDataInJson(fileName, data)
                else:
                    raise sackExceptions(code = "SC-003", message = "Line of Interest (loi) not provided")
                # post alert if countLimit is reached in api
            cv2.imshow(f"frame{bayNo}", results[0].plot())
            # out.write(results[0].plot())
            if cv2.waitKey(1) & 0xFF == ord('q'):  # Press 'q' to exit
                break
            
        lastImageName = f"last_frame_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
        isCountIncorrect =  True
        if countLimit == len(crossedLineSacks["unLoading"]) or countLimit == len(crossedLineSacks["loading"]):
            isCountIncorrect = False
            
        if not alertTriggered  and countLimit != "":
            if countLimit > len(crossedLineSacks["unLoading"]) or countLimit > len(crossedLineSacks["loading"]):
                threading.Thread(target = uploadDataOnCloud, args = (None, None, None, None, None, 
                                bayDetails), kwargs = {"table": table, "url" : sackAnalyticsUrl,
                                "startTime" : startTime, "triggerAlert" : 1, "alertReason" : "count less than Count Limit"} ).start()
            
        threading.Thread(target = uploadDataOnCloud, args = (ftpInfo, ftpFolder, lastImageName, frame, imageFolderName,
                            bayDetails), kwargs = {"table": table, "isClosed": True, "url" : sackAnalyticsUrl,
                                                   "loadingCount" : len(crossedLineSacks["loading"]), "unLoadingCount" :len(crossedLineSacks["unLoading"]),
                            "startTime" : startTime, "isCountIncorrect" :isCountIncorrect} ).start()
        # uploadDataOnCloud(
        #     ftpInfo, ftpFolder, lastImageName, 
        #     frame, imageFolderName, bayDetails, isClosed=True, table=table,
        #     loadingCount=len(crossedLineSacks["loading"]), unLoadingCount=len(crossedLineSacks["unLoading"]),
        #     isCountIncorrect=False, url=sackAnalyticsUrl, startTime= startTime
        # )
        cv2.destroyWindow(f"frame{bayNo}")      
    except sackExceptions as e:
        logger.error(e)
        main.close(bayNo)
        client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "stop due to some error", "statusCode": 400}))
        return None
    except Exception as e:
        logger.error("Error in sackBagCount" + traceback.format_exc())
        main.close(bayNo)
        client.publish("sack/bag/ack", json.dumps({"bayNo": bayNo, "status": "stop due to some error", "statusCode": 400}))
        return None