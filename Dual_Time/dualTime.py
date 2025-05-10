import cv2
import numpy as np 
from ultralytics import YOLO
import time
import threading
import configparser
import os
from datetime import datetime
from utilities import setupDB, VideoCaptureBuffer, setupFtp
import utilities as util
import logging
import traceback

# access config  file
config_path = os.path.join(os.getcwd(),"Dual_Time" ,"config.ini")
config =  configparser.ConfigParser()
config.read(config_path)

# acces Logger File
logger = logging.getLogger('dualTime_logger')

def calculateDualTime(id, allPeronPresentTime, allPersonsPresent, idTimeMapping, incTime):
    try:
        if id not in allPersonsPresent:
            allPersonsPresent.append(id)
            idTimeMapping[id] = f"{int(time.time())}_{id}"
        allPeronPresentTime[id] =  allPeronPresentTime.get(id,0) + incTime
        return allPeronPresentTime[id]
    except Exception as e:
        logger.error(f"Error in calculateDualTime: {e}")
        return 0
    
def fetchStartTime(ftp, folderName, url, booth, table):
    try:
        currentTime = datetime.now()
        startTime = currentTime.time()
        
        if config["Dual-Time"].get("startTime") is not None:
            startTime = datetime.strptime(config["Dual-Time"]["startTime"], "%H:%M:%S")
        else:
            config["Dual-Time"]["startTime"] = startTime.strftime("%H:%M:%S")
            
        startDate = datetime.strptime(config["Dual-Time"].get("startDate", currentTime.date().strftime("%Y-%m-%d")), "%Y-%m-%d")
        endTime = datetime.strptime(config["Dual-Time"].get("endTime", "23:59:59"), "%H:%M:%S")
        endCombineDate = datetime.combine(startDate, endTime.time())
        combineDate = datetime.combine(startDate, startTime.time())
        
        if endCombineDate < currentTime:
            # saveDataInDB(fileName, rois, cameraInfo.get("camera_id"))
            sendPreviousData(ftp , folderName, url,booth, table)
            combineDate = datetime.combine(currentTime.date(), startTime.time())
        config["Dual-Time"]["start_Date"] = currentTime.date().strftime("%Y-%m-%d")
        with open("config.ini", 'w') as configfile:
                config.write(configfile)
        return combineDate, endCombineDate 
    except Exception as e:
        logger.error(f"Error in fetchStartTime: {e}")
        return None, None

def saveDataInLocalDB(conn, api_data):
    try:
        cursor = conn.cursor
        cursor.execute('''INSERT INTO DualTime_Ananlytics (companyCode, exhibitionCode, boothCode, alertType, filepath, mimeType, alert_status, dateandtime) 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                          (api_data["company_code"], api_data["exhibition_code"], api_data["booth_code"], api_data["alert_type"], 
                           api_data["filepath"], api_data["mime_type"], api_data["alert_status"], api_data["dateandtime"]))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error in saveDataInLocalDB: {e}")
        conn.close()
        return False
    
def sendData(ftp,folderName, url, frame, comp, exhinbit, booth, camId,alertType = "dualTime",  table = None):
    try:
        if alertType == "dualTime":
            alertType = 9
        else:
            alertType = 3
            
        timeStamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        ftpFileName  = f"{comp}_{exhinbit}_{booth}_{timeStamp}_{camId}_{alertType}.jpg"
        ftpPath = config["FTP"].get("ftp_location")
        ftpLocation = os.path.join(ftpPath,booth, datetime.now().date().strftime("%Y-%m-%d"), ftpFileName)
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        ftpRes = util.uploadFileOnFtp(ftp, frame, ftpLocation)
        if not ftpRes:
            logger.error("Error in uploadFileOnFtp")
            subFolderName = f"{folderName}/{datetime.now().date()}"
            if not os.path.exists(subFolderName):
                os.makedirs(subFolderName)
            imagePath = f"{subFolderName}/{ftpFileName}"
            success = cv2.imwrite(imagePath, frame)
            if success:
                logging.error(f"Image saved to {imagePath}")
            else:
                logging.error(f"Failed to save image to {imagePath}")
            if ftp is not None:
                ftp.close()
            ftp = setupFtp(config["FTP"]["userName"], config["FTP"]["password"], config["FTP"]["host"], int(config["FTP"]["port"]))
                        
        api_data = {
            "company_code": comp,
            "exhibition_code": exhinbit,            
            "booth_code": booth,
            "alert_type": alertType,
            "dateandtime": created_at,
            "filepath": ftpLocation,
            "mime_type": "image/jpg",
            "alert_status": "pending",
        }
        res = util.sendRequest(url, api_data)
        if not res:
            try:
                conn = setupDB(table)
            except Exception as e:
                logger.error(f"Error in setupDB: {e}")
            saveDataInLocalDB(conn, api_data)
    except Exception as e:
        logger.error(f"Error in sendData: {e}\n{traceback.format_exc()}")
        return None
    
def sendPreviousData(ftp, folderName, url,booth, table = None):
    try:
        if os.path.exists(folderName):
            for subfolder in os.listdir(folderName):
                imageFolder = os.path.join(folderName, subfolder)
                images = [f for f in os.listdir(imageFolder) if f.lower().endswith(".jpg")]
                for image in images:
                    ftpPath = config["FTP"].get("ftp_location")
                    ftpLocation = os.path.join(ftpPath,booth,subfolder, image)
                    frame  = cv2.imread(os.path.join(imageFolder, image))
                    ftpres = util.uploadFileOnFtp(ftp, frame, ftpLocation)
                    if ftpres:
                        os.remove(os.path.join(imageFolder, image))
                    else:
                        if ftp is not None:
                            ftp.close()
                        ftp = setupFtp(config["FTP"]["userName"], config["FTP"]["password"], config["FTP"]["host"], int(config["FTP"]["port"]))
                if not os.listdir(imageFolder):
                    os.rmdir(imageFolder)
        try:
            conn = setupDB(table)
        except Exception as e:
            logger.error(f"Error in setupDB: {e}")
            
        cursor = conn.cursor
        rows = cursor.execute("SELECT * FROM DualTime_Ananlytics").fetchall()
        for row in rows:
            api_data = {
                "company_code": row[1],
                "exhibition_code": row[2],
                "booth_code": row[3],
                "alert_type": row[4],
                "dateandtime": row[8],
                "filepath": row[5],
                "mime_type": row[6],
                "alert_status": row[7],
            }
            res = util.sendRequest(url, api_data)
            if res:
                conn.execute("DELETE FROM DualTime_Ananlytics WHERE id = ?", (row[0],))
                conn.commit()
                conn.close()
                    
    except Exception as e:
        logger.error(f"Error in sendPreviousData: {e}")
        conn.close()
        return None

def detectDualTime(cameraInfo, frameWidth, frameHeight):
    try:
        video , rois , cameraId = cameraInfo.get("rtsp_url"), cameraInfo.get("rois"), cameraInfo.get("camera_id")
        comp, exhibit, booth = config["Company-Details"].get("company_code"), config["Company-Details"].get("exhibition_code"), config["Company-Details"].get("booth_code")
        url  = config["URLS"].get("alertApi")
        allPeronPresentTime, allPersonsPresent , idTimeMapping = {}, {}, {}
        
        timeThresholdForDualTime = int(config["Dual-Time"].get("threshholddualtimeinsec", 120))
        timeThresholdForPersonPresent = int(config["Dual-Time"].get("thresholdpersonpresentinsec", 120))
        
        fileName = f"DualTime_{comp}_{exhibit}_{booth}_{datetime.now().date()}.json"
        
        model = YOLO("yolov8n.pt")
        cap = VideoCaptureBuffer(video)
        
        table = '''create table IF NOT EXISTS DualTime_Ananlytics 
                (id INTEGER  primary key AUTOINCREMENT,companyCode varchar(40),exhibitionCode VARCHAR(50),boothCode VARCHAR(50) ,
                alertType int(10), filepath varchar(50),mimeType varchar(20), alert_status varchar(20),
                dateandtime timestamp , currentTime timeStamp Default current_timestamp)'''
                         
        folderName = f"Dual_Time/DualTime"
        if not os.path.exists(folderName):
            os.makedirs(folderName)
        
        ftpFolder = f"{config['FTP']['ftp_location']}/{booth}/{datetime.now().date()}"        
        ftp = setupFtp(config["FTP"]["userName"], config["FTP"]["password"], config["FTP"]["host"], int(config["FTP"]["port"]))
        ftp.ftp_mkdir_recursive(ftpFolder)
        
        startTime, endCombineDate = fetchStartTime(ftp, folderName, url, booth, table)
        if datetime.now().minute % 5 == 0:
                threading.Thread(target =  sendPreviousData, args = (ftp, folderName, url,booth),kwargs={'table': table}).start()
                syncTime = int(time.time())
        
        personabsentTime = 0
        alertAlreadyDone = {}
        syncTime = int(time.time())
        while datetime.now() < endCombineDate:
            ret, frame = cap.read()
            if not ret:
                cap.release()
                cap = VideoCaptureBuffer(video)
                time.sleep(1)
                continue
            
            frame = cv2.resize(frame, (frameWidth, frameHeight))
            totalPersonPresent = 0 
            
            fps =  cap.cap.get(cv2.CAP_PROP_FPS)
            incTime = 1/fps if fps > 0 else 0.04
            
            
            results =  model.track(frame,imgsz = 640, conf=0.2,persist=True, iou = 0.4, tracker = "bytetrack.yaml", verbose =False)
            if results is None:
                continue
            
            for roi in rois.keys():
                newFrame = frame.copy()
                pts_list = [np.array([[int(p["x"]), int(p["y"])] for p in rois.get(roi)], dtype=np.int32)]
                newFrame = cv2.polylines(newFrame, pts_list, 
                            True, (255, 0, 0), 2)
                personIds = []
                if allPeronPresentTime.get(roi) is None:
                    allPeronPresentTime.update({roi: {}})
                
                if allPersonsPresent.get(roi) is None:
                    allPersonsPresent.update({roi: []})
                    
                if idTimeMapping.get(roi) is None:
                    idTimeMapping.update({roi: {}})
                
                if alertAlreadyDone.get(roi) is None:
                    alertAlreadyDone.update({roi: []})
                    
                for result in results:
                    for j,box in enumerate(result.boxes):
                        classId = box.cls[0]
                        personTime = 0
                        if int(classId) == 0 and box.id is not None:
                            x, y, w, h = map(int, box.xywh[0])
                            if util.personInsidePolygon(rois.get(roi), (x, y)) and roi == "DualTime":
                                id = box.id[0].item()
                                personIds.append(id)
                                personTime = calculateDualTime(id, allPeronPresentTime[roi], allPersonsPresent[roi], idTimeMapping[roi], incTime)
                                newFrame = cv2.putText(newFrame, f"id: {id} Time: {int(personTime)}", (int(rois.get(roi)[2]["x"]), int(rois.get(roi)[2]["y"])), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
                                if personTime > timeThresholdForDualTime:
                                    if id not in alertAlreadyDone[roi]:
                                        threading.Thread(
                                            target=sendData,
                                            args=(ftp, folderName, url, newFrame, comp, exhibit, booth, cameraId),
                                            kwargs={'alertType': 'dualTime', 'table': table}
                                        ).start()
                                        alertAlreadyDone[roi].append(id)
                                    # util.saveDataInFile(fileName, personTime, idTimeMapping[roi][id], roi)
                            if util.personInsidePolygon(rois.get(roi), (x, y)) and roi != "DualTime":
                                totalPersonPresent +=1
                if roi!="DualTime":
                    if totalPersonPresent == 0:
                        personabsentTime += incTime
                        if personabsentTime > timeThresholdForPersonPresent:
                            personabsentTime = 0
                            threading.Thread(
                                        target=sendData,
                                        args=(ftp, folderName, url, newFrame, comp, exhibit, booth, cameraId),
                                        kwargs={'alertType': 'personPresent', 'table': table}
                                    ).start()
                            # util.saveDataInFile(fileName, personabsentTime, idTimeMapping[roi][id], roi)
                    else:
                        personabsentTime = 0
                # cv2.imshow("Dual Time", newFrame)
                
                if cv2.waitKey(1) & 0xFF == ord('q'):  # Press 'q' to exit
                    break
            if int(time.time()) - syncTime > 10:
                threading.Thread(target =  sendPreviousData, args = (ftp, folderName, url,booth),kwargs={'table': table}).start()
                syncTime = int(time.time())
                                
    except Exception as e:
        logger.error(f"Error in detectDualTime: {e}\n{traceback.format_exc()}")