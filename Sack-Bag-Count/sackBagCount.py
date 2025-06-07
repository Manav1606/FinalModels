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

config_path = os.path.join(os.getcwd() ,"Sack-Bag-Count","config.ini")
config =  configparser.ConfigParser()
config.read(config_path)
logger = logging.getLogger("sackBag_logger")

startTime = None

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
    
def saveDataInLocalDB(conn, data, startTime, isClosed):
    try:
        if not conn:
            logger.error("Connection is None")
            return
        cursor = conn.cursor()
        conn.execute('''select id from sackBag_Analytics where companyCode = ? and storeCode = ? and bayCode = ? and countingStartTime = ?''',(data.get("company_code"), data.get("store_code"), data.get("bay_code"), startTime))
        res = cursor.fetchone()
        if res:
            conn.execute('''update sackBag_Analytics set loadingCount = ?, unloadingCount = ?, lastFrameFilepath = ?, countingEndTime = ? where id = ?''',
                         (data.get("loading_count"), data.get("unloading_count"), data.get("last_frame"), startTime, res[0]))
        elif isClosed:
            conn.execute('''insert into sackBag_Analytics (companyCode, storeCode, bayCode, loadingCount, unLoadingCount, lastFrameFilepath, countingEndTime) values (?, ?, ?, ?, ?, ?, ?)''',
                         (data.get("company_code"), data.get("store_code"), data.get("bay_code"), data.get("loading_count"), data.get("unloading_count"), data.get("last_frame"), startTime))
        else:
            conn.execute('''insert into sackBag_Analytics (companyCode, storeCode, bayCode, noOfCounts, vehicleNumber, firstFrame, countingStartTime) values (?, ?, ?, ?, ?, ?, ?)''',
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
    url = None
    ):
    try:
        ftp = utilities.setupFtp(ftpInfo.get("username"), ftpInfo.get("password"), ftpInfo.get("host"), ftpInfo.get("port"))
        fileName = f"{folderName}/{imageName}"
        if frame:
            ftpRes = utilities.uploadFileOnFtp(ftp, fileName, frame)
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
            
        if not isClosed:
            startTime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            apiData = {
                "company_code": bayDetail.get("companyCode"),
                "store_code": bayDetail.get("storeCode"),
                "bay_code": bayDetail.get("bayCode"),
                "no_of_counts": bayDetail.get("noOfCounts", 0),
                "vehicle_number": bayDetail.get("vehicleNumber", None),
                "first_frame": f"ftp://ftp.ttpltech.in//{fileName}",
                "counting_start_time": startTime,
            }
        else:
 
            apiData = {
                "company_code": bayDetail,
                "store_code": bayDetail.get("storeCode"),
                "bay_code": bayDetail.get("bayCode"),
                "loading_count": loadingCount,
                "unloading_count": unLoadingCount,
                "is_count_incorrect": isCountIncorrect,
                "last_frame": f"ftp://ftp.ttpltech.in//{fileName}",
                "counting_end_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }
        
        res = utilities.sendRequest(url, apiData)
        if not res:
            try:
                conn = utilities.setupDB(table)
            except Exception as e:
                logger.error(f"Error in setupDB: {e}")
            saveDataInLocalDB(conn, apiData, startTime, isClosed)
            
    except Exception as e:
        logger.error(f"Error uploading data on cloud: {e}")
        return None
    return

def sendPreviousDataOnCloud(ftpInfo, ftpFolder, imageFolderName, bayDetails, table = None, url = None):
    try:
        ftp = utilities.setupFtp(ftpInfo.get("username"), ftpInfo.get("password"), ftpInfo.get("host"), ftpInfo.get("port"))
        if not ftp:
            logger.error("FTP connection failed")
            return
        
        files = ftp.list(ftpFolder)
        if not files:
            logger.error(f"No files found in FTP folder: {ftpFolder}")
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
                            frame  = cv2.imread(os.path.join(baySubFolder, image))
                            ftpres = utilities.uploadFileOnFtp(ftp, frame, ftpImageLocation)
                            if ftpres:
                                os.remove(os.path.join(baySubFolder, image))
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
            
        if url:
            apiData = {
                "company_code": bayDetails.get("companyCode"),
                "store_code": bayDetails.get("storeCode"),
                "bay_code": bayDetails.get("bayCode"),
                "image_folder": imageFolderName,
            }
            res = utilities.sendRequest(url, apiData)
            if not res:
                conn = utilities.setupDB(table)
                saveDataInLocalDB(conn, apiData, datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
        
        ftp.close()
        
    except Exception as e:
        logger.error(f"Error in sendPreviousDataOnCloud: {e}")
    return

def sackBagCount(bayDetails, rtsp, direction, frameWidth, frameHeight,modelName, stopEvent,ftpInfo , sackAnalyticsUrl, countLimit = 0, loi=None, roi=None, client = None, table = None):
    try:
        logger.info("starting sackBagCount thread")
        
        bayNo = bayDetails.get("bayNo")
        companyCode = bayDetails.get("companyCode")
        storeCode = bayDetails.get("storeCode")
        
        if not os.path.exists("sack_data"):
            os.makedirs("sack_data")
        
        imageFolderName = f"sack_data/sack_bag_frames/{companyCode}_{storeCode}_{bayNo}"
        if not os.path.exists(imageFolderName):
            os.makedirs(imageFolderName)
        
        ftpFolder = f"{ftpInfo.get('folder')}/{companyCode}/{storeCode}/{bayNo}"
        
        try:
            ftp = utilities.setupFtp(ftpInfo.get("username"), ftpInfo.get("password"), ftpInfo.get("host"), ftpInfo.get("port"))
            ftp.ftp_mkdir_recursive(ftpFolder)
        except Exception as e:
            logger.error(f"Error setting up FTP: {e}")
            return None
            
        if rtsp:
            cap = cv2.VideoCapture(rtsp)
            while not cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    logger.error("Failed to connect to RTSP stream, retrying...")
                    time.sleep(1)
                    cap = cv2.VideoCapture(rtsp)
                    continue
                else:
                    try:
                        imageName = f"first_frame_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
                        uploadDataOnCloud(
                            ftpInfo, ftpFolder, imageName, frame, imageFolderName, 
                            bayDetails, table = table, url = sackAnalyticsUrl,
                        )
                        
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
        
        while not stopEvent.is_set():
            
            if stopEvent.is_set():
                logger.info("Stopping sackBagCount thread")
                break
            
            ret, frame = cap.read()
            if not ret:
                cap.release()
                cap = cv2.VideoCapture(rtsp)
                time.sleep(1)
                continue
            frame = cv2.resize(frame, (frameWidth, frameHeight))
            results =  model.track(frame,imgsz = 640, conf=0.2,persist=True, iou = 0.4, tracker = "bytetrack.yaml", verbose =False)
            # frame = cv2.line(frame, (int(loi[0][0]),int(loi[0][1]) ), (int(loi[1][0]), int(loi[1][1])), color=(0, 255, 0), thickness=2)
            objectCoordinates = utilities.fetchObject(results, objects=[0], roi = roi)
            if objectCoordinates.get(0) is not None:
                if loi is not None:
                    countSacks(objectCoordinates.get(0),uncrossedLineSacks, crossedLineSacks, direction, loi[0], loi[1])
                    # publish data to MQTT broker
                    publish = {}
                    data  = {
                        "unloadingSacks": len(crossedLineSacks["unLoading"]),
                        "loadingSacks": len(crossedLineSacks["loading"]),
                    }
                    publish[bayNo] = data
                    if client:
                        client.publish("sack-bag-counter",json.dumps(publish))
                        
                    utilities.saveDataInJson(fileName, data)
                else:
                    raise sackExceptions(code = "SC-003", message = "Line of Interest (loi) not provided")
                # post alert if countLimit is reached in api
            cv2.imshow(f"frame{bayNo}", results[0].plot())
            # out.write(results[0].plot())
            if cv2.waitKey(1) & 0xFF == ord('q'):  # Press 'q' to exit
                break
            
        lastImageName = f"last_frame_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
        uploadDataOnCloud(
            ftpInfo, ftpFolder, lastImageName, 
            frame, imageFolderName, bayDetails, isClosed=True, table=table,
            loadingCount=len(crossedLineSacks["loading"]), unLoadingCount=len(crossedLineSacks["unLoading"]),
            isCountIncorrect=False, url=sackAnalyticsUrl
        )
        cv2.destroyWindow(f"frame{bayNo}")      
    except sackExceptions as e:
        logger.error(e)
        return None
    except Exception as e:
        logger.error(f"Error in sackBagCount: {e}")
        return None