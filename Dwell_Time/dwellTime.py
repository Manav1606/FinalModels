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
import json

# access config  file
appFolder = "/home/transline/Documents/storepulse_dwellTime"
config_path = f"{appFolder}/config.ini"
config =  configparser.ConfigParser()
config.read(config_path)

# acces Logger File
logger = logging.getLogger('dwellTime_logger')

def calculateDwellTime(id, allPeronPresentTime, allPersonsPresent, idTimeMapping, incTime):
    try:
        if id not in allPersonsPresent:
            allPersonsPresent.append(id)
            idTimeMapping[id] = f"{int(time.time())}_{id}"
        allPeronPresentTime[id] =  allPeronPresentTime.get(id,0) + incTime
        return allPeronPresentTime[id]
    except Exception as e:
        logger.error(f"Error in calculateDwellTime: {e}")
        return 0
    
def saveDataInLocalDB(conn, api_data, eventType = "waitingTime"):
    try:
        cursor = conn.cursor
        if eventType == "waitingTime":
            cursor.execute('''INSERT INTO DwellTime (companyCode, exhibitionCode, boothCode, cameraId, data) 
                              VALUES (?, ?, ?, ?, ?)''', 
                              (api_data["company_code"], api_data["exhibition_code"], api_data["booth_code"], 
                               api_data["camera_id"], json.dumps(api_data["data"])))
        else:
            cursor.execute('''INSERT INTO DwellTime_Ananlytics (companyCode, exhibitionCode, boothCode, alertType, filepath, mimeType, alert_status, dateandtime, remark) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                            (api_data["company_code"], api_data["exhibition_code"], api_data["booth_code"], api_data["alert_type"], 
                            api_data.get("filepath", ""), api_data.get("mime_type", ""), api_data.get("alert_status", ""), api_data["dateandtime"], api_data.get("remark", "")))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error in saveDataInLocalDB: {e}")
        conn.close()
        return False
def sendData(folderName, url, frame, comp, exhinbit, booth, camId,alertType = "dwellTime",  table = None, waitingTimeData = None, eventType = "waitingTime"):
    try:    
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        ftpLocation = None
        if frame is not None and eventType != "waitingTime":
            try:      
                ftp = setupFtp(config["FTP"]["userName"], config["FTP"]["password"], config["FTP"]["host"], int(config["FTP"]["port"]))
            except Exception as e:
                logger.error(f"error in making directory {e}") 
                
            timeStamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
            ftpFileName  = f"{comp}_{exhinbit}_{booth}_{timeStamp}_{camId}_{alertName}.jpg"
            ftpPath = config["FTP"].get("ftp_location")
            # ftpLocation = os.path.join(ftpPath,booth, datetime.now().date().strftime("%Y-%m-%d"), ftpFileName)
            ftpLocation = f"{ftpPath}/{booth}/{datetime.now().date().strftime('%Y-%m-%d')}/{ftpFileName}"
            ftpRes = util.uploadFileOnFtp(ftp, frame, ftpLocation)
            if not ftpRes:
                logger.error("Error in uploadFileOnFtp")
                subFolderName = f"{folderName}/{datetime.now().date()}"
                if not os.path.exists(subFolderName):
                    os.makedirs(subFolderName)
                imagePath = f"{subFolderName}/{ftpFileName}"
                success = cv2.imwrite(imagePath, frame)
                if success:
                    logger.error(f"Image saved to {imagePath}")
                else:
                    logger.error(f"Failed to save image to {imagePath}")
                
            if ftp is not None:
                ftp.close()
                        
        api_data = {
            "company_code": comp,
            "exhibition_code": exhinbit,            
            "booth_code": booth,
        }
        
        if eventType == "waitingTime":
            api_data.update({
                "camera_id": camId,
                "data": waitingTimeData,
            })
        else:
            if alertType == "dwellTime":
                alertType = 9
                alertName = "dwellTime"
            else:
                alertType = 3
                alertName = "staff_absent"
                
            api_data.update({
                "alert_type": alertType,
                "dateandtime": created_at,
                "alert_status": "pending",
                "filepath": f"ftp://ftp.ttpltech.in//{ftpLocation}",
                "mime_type": "image/jpg",
            })
            
        res = util.sendRequest(url, api_data)
        if not res:
            try:
                conn = setupDB(table)
            except Exception as e:
                logger.error(f"Error in setupDB: {e}")
            saveDataInLocalDB(conn, api_data, eventType = eventType)
    except Exception as e:
        logger.error(f"Error in sendData: {e}\n{traceback.format_exc()}")
        return None
    
def sendPreviousData(folderName,booth, operationConfigs = None):
    try:
        if config.has_section("FTP"):
            if os.path.exists(folderName):
                if len(os.listdir(folderName)) > 0:   
                    try:      
                        ftp = setupFtp(config["FTP"]["userName"], config["FTP"]["password"], config["FTP"]["host"], int(config["FTP"]["port"]))
                    except Exception as e:
                        logger.error(f"error in making directory {e}")
                    if os.path.exists(folderName):
                        for subfolder in os.listdir(folderName):
                            # imageFolder = os.path.join(folderName, subfolder)
                            imageFolder = f"{folderName}/{subfolder}"
                            images = [f for f in os.listdir(imageFolder) if f.lower().endswith(".jpg")]
                            ftpPath = config["FTP"].get("ftp_location")
                            # ftpLocation = os.path.join(ftpPath,booth,subfolder)
                            ftpLocation = f"{ftpPath}/{booth}/{subfolder}"
                            ftp.ftp_mkdir_recursive(ftpLocation)
                            for image in images:
                                # ftpImageLocation = os.path.join(ftpLocation, image)
                                ftpImageLocation = f"{ftpLocation}/{image}"
                                # frame  = cv2.imread(os.path.join(imageFolder, image))
                                frame  = cv2.imread(f"{imageFolder}/{image}")
                                if frame is None:
                                    logger.error(f"Failed to read image {image} in {imageFolder}")
                                    continue
                                ftpres = util.uploadFileOnFtp(ftp, frame, ftpImageLocation)
                                if ftpres:
                                    # os.remove(os.path.join(imageFolder, image))
                                    os.remove(f"{imageFolder}/{image}")
                                else:
                                    if ftp is not None:
                                        ftp.close()
                                    ftp.connect()
                            if not os.listdir(imageFolder):
                                os.rmdir(imageFolder)
                    if ftp is not None:
                        ftp.close()
        for operationConfig in operationConfigs.values():
            try:
                conn = setupDB(operationConfig.get('table'))
            except Exception as e:
                logger.error(f"Error in setupDB: {e}")
                
            cursor = conn.cursor
            rows = cursor.execute(f"SELECT * FROM {operationConfig.get('tableName')}").fetchall()
            for row in rows:
                if operationConfig.get('tableName') == "DwellTime":
                    api_data = {
                        "company_code": row[1],
                        "exhibition_code": row[2],
                        "booth_code": row[3],
                        "camera_id": row[4],
                        "data": json.loads(row[5]),
                    }
                else:
                    api_data = {
                        "company_code": row[1],
                        "exhibition_code": row[2],
                        "booth_code": row[3],
                        "alert_type": row[4],
                        "dateandtime": row[8],
                        "filepath": row[5],
                        "mime_type": row[6],
                        "alert_status": row[7],
                        "remark": row[9] if row[9] else "",
                    }
                res = util.sendRequest(operationConfig.get('url'), api_data)
                if res:
                    conn.execute(f"DELETE FROM {operationConfig.get('tableName')} WHERE id = ?", (row[0],))
                    conn.commit()
                conn.close()
                    
    except Exception as e:
        logger.error(f"Error in sendPreviousData: {e}")
        conn.close()
        return None

def sendInactivePersonsWaitingTime(personIds, allPeronPresentTime, comp, exhibit, booth, camId, url = None, table = None, activeIds = {}, fps = 30):
    try:
        inactivePersons = {}
        allIds = []
        for id in allPeronPresentTime.keys():
            if id not in personIds and allPeronPresentTime.get(id) > 25:
                if activeIds.get(id) is not None and int(activeIds.get(id)) > 2:
                    inactivePersons[id] = allPeronPresentTime.get(id)
                    allIds.append(id)
                else:
                    if activeIds.get(id) is None:
                        activeIds[id] = 0
                    activeIds[id] +=1/fps
            else:
                if id in activeIds:
                    activeIds.pop(id, None)
        # inactivePersons = { id: allPeronPresentTime.get(id) for id in allPeronPresentTime.keys() if id not in personIds and allPeronPresentTime.get(id) > 25}
        for id in allIds:
            if id not in personIds:
                allPeronPresentTime.pop(id, None)
                activeIds.pop(id, None)
        # for id in inactivePersons.keys():
        #     allPeronPresentTime.pop(id, None)
        if inactivePersons == {}:
            return 
        waitingTimeData = []
        for id in inactivePersons.keys():
            waitingTimeData.append({
                "person_id": id,
                "Waiting_time_seconds": int(inactivePersons[id]),
            })
        # print("waiting Time Data: ", waitingTimeData)
        threading.Thread(target = sendData, args = (None, url, None, comp, exhibit, booth, camId), kwargs= {'alertType' :"waitingTime", 'table' : table, 'waitingTimeData' : waitingTimeData, 'eventType' : "waitingTime" }).start()
        # sendData(None, url, None, comp, exhibit, booth, camId, alertType= "waitingTime", table = table, waitingTimeData = json.dumps(inactivePersons))
    except Exception as e:
        logger.error(f"Error in sendInactivePersonsWaitingTime: {e}")
        return None

def detectDwellTime(cameraInfo, frameWidth, frameHeight,startTime, endTime, folderName, operationConfigs):
    try:
        video , rois , cameraId, cameraName = cameraInfo.get("rtsp_url").replace("#", "%23"), cameraInfo.get("rois"), cameraInfo.get("camera_id"), cameraInfo.get("camera_name")
        print("video source: ", video)
        logger.info(f"started Camera {cameraId}")
        comp, exhibit, booth = config["Company-Details"].get("company_code"), config["Company-Details"].get("exhibition_code"), config["Company-Details"].get("booth_code")
        
        allPeronPresentTime, allPersonsPresent , idTimeMapping = {}, {}, {}
        timeThresholdForDwellTime = int(config["Dwell-Time"].get("thresholdDwellTimeInsec", 120))
        timeThresholdForPersonPresent = int(config["Dwell-Time"].get("thresholdpersonpresentinsec", 120))
        coolDownTime = int(config["Dwell-Time"].get("coolDownTimeInsec", 60))
        coolDown = coolDownTime
        
        
        model = YOLO("yolov8n.pt")
        cap = VideoCaptureBuffer(video)
                         
        if not os.path.exists(folderName):
            os.makedirs(folderName)
        
        ftpFolder =  None
        if config.has_section("FTP"):
            print("connecting to FTP")
            ftpFolder = f"{config['FTP']['ftp_location']}/{booth}/{datetime.now().date()}"  
            try:      
                ftp = setupFtp(config["FTP"]["userName"], config["FTP"]["password"], config["FTP"]["host"], int(config["FTP"]["port"]))
                ftp.ftp_mkdir_recursive(ftpFolder)
                ftp.close()
            except Exception as e:
                logger.error(f"error in making directory {e}")
            
        
        personabsentTime = 0
        alertAlreadyDone = {}
        syncTime = int(time.time())
        lastFrameTime =  datetime.now()
        # cameraLastResponseTime = 0
        activeIds = {}
        targetFps = 20
        frameCount = 0
        
        while datetime.now().time() >= startTime and datetime.now().time() < endTime:
            ret, frame = cap.read()
            if not ret:
                cap.release()
                cap = VideoCaptureBuffer(video)
                time.sleep(1)
                continue
                
            fps = cv2.CAP_PROP_FPS
            skipFrames = int(fps / targetFps) if fps > 0 else 1
            frameCount += 1
            if skipFrames > 0 :
                if frameCount % skipFrames != 0:
                    continue
                
            frame = cv2.resize(frame, (frameWidth, frameHeight))
            totalPersonPresent = 0 
            
            now = datetime.now()
            incTime = (now - lastFrameTime).total_seconds()
            lastFrameTime = now
            
            fps =  cap.cap.get(cv2.CAP_PROP_FPS)
            # incTime = 1/fps if fps > 0 else 0.04
            
            results =  model.track(frame,imgsz = 640, conf=0.2,persist=True, iou = 0.4, tracker = "bytetrack.yaml", verbose =False)
            if results is None:
                continue
            
            # showFrame = frame.copy()
            # for roi in rois.keys():
            #     if roi == "dwellTime":
            #         color = (255, 0, 0)
            #     elif roi == "waitingTime":
            #         color = (255, 255, 0)
            #     else:
            #         color = (0, 0, 255)
            #     pts_list = [np.array([[int(p["x"]), int(p["y"])] for p in rois.get(roi)], dtype=np.int32)]
            #     showFrame = cv2.polylines(showFrame, pts_list, 
            #             True, color, 2)
            #     showFrame = cv2.putText(showFrame, f"name {roi}", (int(rois.get(roi)[0]["x"]), int(rois.get(roi)[0]["y"])), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
                
                
            for roi in rois.keys():
                newFrame = frame.copy()
                pts_list = [np.array([[int(p["x"]), int(p["y"])] for p in rois.get(roi)], dtype=np.int32)]
                if roi == "dwellTime":
                    color = (255, 0, 0)
                elif roi == "waitingTime":
                    color = (255, 255, 0)
                else:
                    color = (0, 0, 255)
                newFrame = cv2.polylines(newFrame, pts_list, 
                            True, color, 2)
                # showFrame = cv2.polylines(showFrame, pts_list, 
                #             True, color, 2)
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
                            # showFrame = cv2.putText(showFrame, f"Id: {box.id[0].item()}", (x, y), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
                            if util.personInsidePolygon(rois.get(roi), (x, y)) and (roi == "dwellTime" or roi == "waitingTime"):
                                id = box.id[0].item()
                                personIds.append(id)
                                personTime = calculateDwellTime(id, allPeronPresentTime[roi], allPersonsPresent[roi], idTimeMapping[roi], incTime)
                                # calculate person Time and store in the file and send it to the api or if we want to store then send all allpersonTime to api
                                if roi == "dwellTime":
                                    if personTime > timeThresholdForDwellTime:
                                        if id not in alertAlreadyDone[roi]:
                                            x, y, top_left,bottom_right  = util.fetchTextScale(int(rois.get(roi)[2]["x"]), int(rois.get(roi)[2]["y"]), text = f"Time(in sec): {int(personTime)}" )
                                            cv2.rectangle(newFrame, top_left, bottom_right, (255, 255, 255), thickness=cv2.FILLED)
                                            newFrame = cv2.putText(newFrame, f"Time(in sec): {int(personTime)}", (x, y), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
                                            threading.Thread(
                                                target=sendData,
                                                args=(folderName, operationConfigs.get("alertOperations").get("url"), newFrame, comp, exhibit, booth, cameraId),
                                                kwargs={'alertType': 'dwellTime', 'table': operationConfigs.get("alertOperations").get("table"), 'eventType': "dwellTime"}
                                            ).start()
                                            alertAlreadyDone[roi].append(id)
                                        # util.saveDataInFile(fileName, personTime, idTimeMapping[roi][id], roi)
                            if util.personInsidePolygon(rois.get(roi), (x, y)) and roi != "dwellTime":
                                totalPersonPresent +=1
                if roi == "waitingTime":
                    sendInactivePersonsWaitingTime(personIds, allPeronPresentTime[roi], comp, exhibit, booth, cameraId, table = operationConfigs.get("dwellTime").get("table"), url = operationConfigs.get("dwellTime").get("url"), activeIds = activeIds, fps = fps)
                    # threading.Thread(
                    #     target=sendInactivePersonsWaitingTime, 
                    #     args = (personIds, allPeronPresentTime[roi], comp, exhibit, booth, cameraId ), 
                    #     kwargs = {'table': table, 'url': url}).start()
                elif roi!="dwellTime" and roi != "waitingTime":
                    if totalPersonPresent == 0:
                        personabsentTime += incTime
                        if personabsentTime > timeThresholdForPersonPresent:
                            if coolDown == coolDownTime:
                                # personabsentTime = 0
                                x, y, top_left,bottom_right  = util.fetchTextScale(int(rois.get(roi)[0]["x"]), int(rois.get(roi)[0]["y"]) )
                                cv2.rectangle(newFrame, top_left, bottom_right, (255, 255, 255), thickness=cv2.FILLED)
                                newFrame = cv2.putText(newFrame, f"STAFF_ABSENT", (x, y), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
                                threading.Thread(
                                            target=sendData,
                                            args=(folderName, operationConfigs.get("alertOperations").get("url"), newFrame, comp, exhibit, booth, cameraId),
                                            kwargs={'alertType': 'personPresent', 'table': operationConfigs.get("alertOperations").get("table"), 'eventType': "dwellTime"}
                                        ).start()
                                # util.saveDataInFile(fileName, personabsentTime, idTimeMapping[roi][id], roi)
                                coolDown -= incTime
                            else:
                                coolDown -=incTime
                                if coolDown<= 0:
                                    coolDown = coolDownTime
                    else:
                        personabsentTime = 0
                        coolDown = coolDownTime
                # cv2.imshow(f"Camera {cameraName}", frame)
                        
                if cv2.waitKey(1) & 0xFF == ord('q'):  # Press 'q' to exit
                    break
            if int(time.time()) - syncTime > 300:
                threading.Thread(target =  sendPreviousData, args = (folderName,booth),kwargs={'operationConfigs': operationConfigs}).start()
                syncTime = int(time.time())
        logger.info("Time Over")   
                                
    except Exception as e:
        logger.error(f"Error in detectDwellTime: {e}\n{traceback.format_exc()}")