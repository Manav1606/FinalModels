import cv2
from ultralytics import YOLO
import threading
import time
import numpy as np
import configparser
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
import sqlite3
import logging
import requests
from ftplib import FTP
from io import BytesIO
import base64
import traceback
#crowd  
# torch.cuda.set_device(0)
config_path = os.path.join(os.getcwd(), "config.ini")

if not os.path.exists(os.path.join(os.getcwd(), f"heatMap_{datetime.now().date()}.log")):
    logging.basicConfig(
        filename=f"heatMap_{datetime.now().date()}.log", 
        level=logging.ERROR,  
        format='%(asctime)s - %(levelname)s - %(message)s',  
        filemode='w'  
    )
else:
    logging.basicConfig(
        filename=f"heatMap_{datetime.now().date()}.log",
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='a'  # Append to the file when it exists
    )

if os.access(config_path, os.R_OK):
        print(f"The user has read permissions for {config_path}")
else:
        print(f"The user does NOT have read permissions for {config_path}")

config =  configparser.ConfigParser()
config.read(config_path)
class VideoCaptureBuffer:
    """Efficiently captures frames in a separate thread to reduce lag and distortion."""
    def __init__(self, video_source):
        self.video_source = video_source
        self.cap = cv2.VideoCapture(video_source)
        self.buffer_frame = None
        self.stopped = False
        self.lock = threading.Lock()
        self.is_rtsp = isinstance(video_source, str) and video_source.startswith("rtsp")

        # Start the frame updating thread
        self.thread = threading.Thread(target=self.update_frames, daemon=True)
        self.thread.start()

    def update_frames(self):
        while not self.stopped:
            ret, frame = self.cap.read()
            if ret:
                with self.lock:
                    self.buffer_frame = frame
                time.sleep(0.01)  # Small delay to prevent CPU overuse
            else:
                if self.is_rtsp:
                    print("Failed to capture frame from RTSP, retrying...")
                    time.sleep(1)
                else:
                    print("Failed to capture frame, reinitializing...")
                    self.cap.release()
                    self.cap = cv2.VideoCapture(self.video_source)
                    time.sleep(1)

    def read(self):
        with self.lock:
            frame = self.buffer_frame
        return frame is not None, frame

    def release(self):
        self.stopped = True
        self.thread.join()
        self.cap.release()
        
class setupServer:
    def __init__(self):
        db_path = os.path.join(os.path.dirname(__file__), 'myDatabase.db')
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''create table IF NOT EXISTS Heatmap_Ananlytics 
                            (id INTEGER  primary key AUTOINCREMENT,camId varchar(40),roi VARCHAR(50) ,averageTime FLOAT, maxTime FLOAT,minTime FLOAT, 
                            date timestamp , currentTime timeStamp Default current_timestamp)''')
        self.commit
    def commit(self):
        self.conn.commit()
    def close(self):
        self.conn.close()   

class setupFtp:
    def __init__(self, userName, password, host, port):
        self.userName = userName
        self.password = password
        self.host = host
        self.port = port
        # self.ftp = FTP(host = self.host, user = self.userName, passwd = self.password, port = self.port)
        try:
            self.ftp = FTP()
            self.ftp.connect(self.host, self.port)
            self.ftp.login(self.userName, self.password)
        except Exception as e:
            logging.error(f"FTP connection failed: {e}")

        
    def sendFile(self,fileName, stream):
        try:
            res = self.ftp.storbinary(f'STOR {fileName}', stream)
            msg = 'Upload %s to FTP Server %s.'
            if res.startswith('226 Transfer complete'):
                logging.error(msg % ('success', self.host))
                return True
            else:
                logging.error(msg % ('falied', self.host)) 
                return False  
        except Exception as e:
            logging.error(f"Error in sendFile: {e}")
            return False
        
    def close(self):
        self.ftp.quit()

def sendRequest(url, data):
    try:
        headers = {
            'content-type': 'application/json',
        }
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            logging.error(f"Data sent successfully to {url}")
            return True
        else:
            logging.error(f"Error in sendRequest: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error in sendRequest: {e}")
        return None

def drawHeatMap(new_person_detected_cordinates, heatmap_accumulator, frame):
    for x, y in new_person_detected_cordinates:
        center_x, center_y = x, y 
        radius = 55
        mask = np.zeros_like(heatmap_accumulator, dtype=np.uint8)
        cv2.circle(mask, (center_x, center_y), radius, 1, thickness=-1)
        heatmap_accumulator[mask == 1] += 8
    
    heatmap_blurred = cv2.GaussianBlur(heatmap_accumulator, (25, 25), 0)
    normalized_heatmap = cv2.normalize(heatmap_blurred, None, 0, 255, cv2.NORM_MINMAX)

    heatmap_uint8 = normalized_heatmap.astype(np.uint8)
    heatmap_colored = cv2.applyColorMap(heatmap_uint8, cv2.COLORMAP_JET)
    # final_heatMap = cv2.resize(heatmap_colored, (frame.shape[1], frame.shape[0]))
    # output_frame = cv2.addWeighted(frame, 1.0, heatmap_colored, 0.4, 0)
    return heatmap_colored

def removePersonId(allPersonsPresent, personIds):
    # notPresentIds =  [id for id in allPersonsPresent.keys() if id not in personIds]
    # for id in notPresentIds:
    #     allPersonsPresent.pop(id)

    for id in list(allPersonsPresent):
        if id not in personIds:
            del allPersonsPresent[id]
            
def savePreviousData(camId):
    try:
        conn = setupServer()
        cursor = conn.cursor
        cursor.execute('''select * from Heatmap_Ananlytics where camId = ?''', (camId,))
        rows = cursor.fetchall()
        if rows:
            data = {}
            for row in rows:
                camId, roi, averageTime, maxTime, minTime, date = row[1:]
                data[roi] = {
                    "averageTime": averageTime,
                    "maxTime": maxTime,
                    "minTime": minTime,
                    "date": date
                }
        # save on the api 
    except Exception as e:
        logging.error(f"Error in savePreviousData in {camId}: {e}")

def saveDataInFile(fileName, data, id, roi):
    try:
        if not fileName.endswith(".json"):
            logging.error("File Name is not json")
            return
        if not data:
            print("Data is empty")
            return
        file_path = Path(fileName)
        if not file_path.exists():
            fileData = {roi: {id: data}}
            with open(file_path, 'w') as f:
                json.dump(fileData, f)
        else:
            with open(file_path, 'r') as f: 
                try:
                    fileData = json.load(f)
                except json.JSONDecodeError:
                    fileData = {}
                if fileData.get(roi) is None:
                    fileData.update({roi: {}})
                if fileData.get(roi).get(id) is None:
                    fileData.get(roi).update({id: data})
            with open(file_path, 'w') as f:
                json.dump(fileData, f)
        return
    except Exception as e:
        logging.error(f"Error in saveDataInFile: {e}") 

def saveDataInDB(fileName, rois,camId):
    try:
    
        filePath = Path(fileName)
        if not filePath.exists():
            logging.error("File not found")
            return                                                                                                             
        
        try:
            conn = setupServer()
            cursor = conn.cursor
        except Exception as e:
            logging.error(f"Error in setupServer in {camId}: {e}")
            return
        
        with open(filePath, 'r') as f:
            data = json.load(f)
            if not data:
                logging.error(f"Data is empty in {camId}")
                for roi in rois:
                    cursor.execute('''INSERT INTO Heatmap_Ananlytics (camId,roi, averageTime, maxTime, minTime,  date) values(?,?, ?, ?,?,?)''', (camId, roi, 0, 0,0, datetime.now()))
                conn.commit()
            else:
                for roi in data.keys():
                    totalTime = 0
                    minTime = float('inf')
                    maxTime = float('-inf')
                    for id, time in data.get(roi).items():
                        totalTime += time
                        if time < minTime and time != 0:
                            minTime = time
                        if time > maxTime:
                            maxTime = time
                    averageTime = totalTime / len(data.get(roi)) if len(data.get(roi)) > 0 else 0
                    if minTime == float('inf'):
                        minTime = 0
                    if maxTime == float('-inf'):
                        maxTime = 0
                    cursor.execute('''INSERT INTO Heatmap_Ananlytics (camId, roi, averageTime, maxTime, minTime,  date) values(?,?, ?, ?,?,?)''', (camId, roi, averageTime, maxTime,minTime, datetime.now()))
                conn.commit()
            
            with open(filePath, 'w') as f:
                json.dump({}, f)
            cursor.execute('''select * from Heatmap_Ananlytics where camId = ?''', (camId,))
            rows = cursor.fetchall()
            print(rows)
            conn.close()
            ####### call a api for cloud and if ok response will come delete all the data from the local db
    except Exception as e:
        logging.error(f"Error in saveDataInDB in {camId}: {e}")
        return
     

def updateImage(ftp, frame,compCode, boothCode , exhibitCode, camId,heatmap_accumulator,finalImage = None):
    if frame is not None or finalImage is not None:
        image_path = f"heatMap_{compCode}_{exhibitCode}_{boothCode}_{camId}_{datetime.now().date()}.jpg"
        ftpPath = f"Storepulse2/HeatMap/{compCode}_{boothCode}_{exhibitCode}_{camId}_{datetime.now().date()}_heatMapImage.jpg"
        cv2.imwrite(image_path, frame)
        np.save(f"{compCode}_{boothCode}_{exhibitCode}_{camId}_{datetime.now().date()}_heatMapImage.npy", heatmap_accumulator)
        success, encoded_image = cv2.imencode('.jpg', frame)
        if success:
            image_bytes = encoded_image.tobytes()
            stream = BytesIO(image_bytes)
            res = ftp.sendFile(ftpPath,stream)
            if res == False:
                ftp.close()
                ftp = setupFtp(config["FTP"]["userName"], config["FTP"]["password"], config["FTP"]["host"], int(config["FTP"]["port"]))
        url = config["URLS"]["save_image"]
        host = config["FTP"]["host"]
        data = {
            "company_code": compCode,
            "exhibition_code": exhibitCode,
            "camera_id": camId,
            "booth_code": boothCode,
            "date_and_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scope":"hotzone",
            "ftp_path": f"ftp://{host}//{ftpPath}",
            "image": None,
        }
        if finalImage is not None:
            success, encoded_image = cv2.imencode('.jpg', finalImage)
            if success:
                img_base64 = base64.b64encode(encoded_image.tobytes()).decode('utf-8')
                data.update({"final_image": img_base64})

        sendRequest(url, data)
    return        
        
def fetchStartDate(rois, cameraInfo,abortInterval,fileName = None):
    currentTime = datetime.now()
    startTime = currentTime.time()
    if config["Heat-Map"].get("start_time") is not None:
       startTime = datetime.strptime(config["Heat-Map"]["start_time"], "%H:%M:%S")
    else:
        config["Heat-Map"]["start_time"] = startTime.strftime("%H:%M:%S")
    startDate = datetime.strptime(config["Heat-Map"].get("start_Date", currentTime.date().strftime("%Y-%m-%d")), "%Y-%m-%d")
    combineDate = datetime.combine(startDate, startTime.time())
    timeDifference = (currentTime - combineDate).total_seconds() / 3600
    if timeDifference >= abortInterval:
        # config["Pose-Estimation"]["startTime"] = currentTime.strftime("%Y-%m-%d %H:%M:%S")
        # with open("config.ini", 'w') as configfile:
        #     config.write(configfile)
        saveDataInDB(fileName, rois, cameraInfo.get("camera_id"))
        combineDate = datetime.combine(currentTime.date(), startTime.time())
    config["Heat-Map"]["start_Date"] = currentTime.date().strftime("%Y-%m-%d")
    with open("config.ini", 'w') as configfile:
            config.write(configfile)
    return combineDate 

def extractImage(frame, points):
    pts = np.array([[int(p["x"]), int(p["y"])] for p in points], dtype=np.int32)
    mask = np.zeros(frame.shape[:2], dtype=np.uint8)
    cv2.fillPoly(mask, [pts], 255)
    # x, y, w, h = cv2.boundingRect(pts)
    # cropped_image = frame[y:y+h, x:x+w]
    # cropped_mask = mask[y:y+h, x:x+w]
    masked_image = cv2.bitwise_and(frame, frame, mask=mask)
    return masked_image    

def megreHeatMapWithOrginalImage(frame, heatMap, rois):
    mask = np.zeros(frame.shape[:2], dtype=np.uint8)
    pts_list = [np.array([[int(p["x"]), int(p["y"])] for p in rois.get(roi)], dtype=np.int32) for roi in rois.keys()]
    cv2.fillPoly(mask, pts_list, 255)
    masked_frame = cv2.bitwise_and(heatMap, heatMap, mask=mask)
    overLapImg = cv2.addWeighted(frame, 1, masked_frame, 0.6, 0)
    return overLapImg 

def personInsidePolygon(points, person):
    pts = np.array([[int(p["x"]), int(p["y"])] for p in points], dtype=np.int32)
    is_inside = cv2.pointPolygonTest(pts, person, False)
    if is_inside >= 0:
        return True
    return False
        
def crowdHeatMap(cameraInfo):
    try:
        video = cameraInfo.get("rtsp_url")
        cap = VideoCaptureBuffer(video)
        model = YOLO("yolov8s.pt")
        frameWidth =  int(config["Heat-Map"]["frame_width"])
        frameHeight = int(config["Heat-Map"]["frame_height"])
        # rois = config["Pose-Estimation"].get("rois","{}")
        rois = cameraInfo.get("rois", {})
        updateFaceInterval = int(config["Heat-Map"].get("update_frame_interval", 300))
        ftp = setupFtp(config["FTP"]["userName"], config["FTP"]["password"], config["FTP"]["host"], int(config["FTP"]["port"]))
        compCode = config["Company-Details"]["company_code"]
        exhibitCode =  config["Company-Details"]["exhibition_code"]
        boothCode = config["Company-Details"]["booth_code"]
        camId = cameraInfo.get("camera_id")
        currentTime = datetime.now()
        prevDate = config["Heat-Map"].get("start_Date", currentTime.date().strftime("%Y-%m-%d"))
        prevFileName = f"{compCode}_{exhibitCode}_{boothCode}_{camId}_{prevDate}_HeatMappingData.json" 
        fileName = f"{compCode}_{exhibitCode}_{boothCode}_{camId}_{datetime.now().date()}_HeatMappingData.json" 
        
        lastUpdatedTime = int(time.time())
        abortInterval = float(config["Heat-Map"].get("abort_interval_in_hours", 24))
        # fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        # out = cv2.VideoWriter('output_new.mp4', fourcc, 15.0, (frameWidth, frameHeight))

        # if isinstance(rois, str):
        #     rois = json.loads(rois)
        if rois == {}:
            rois = {"FS": {"x": [0, frameWidth], "y": [0, frameHeight]}}
            
        imageFileName = f"{compCode}_{boothCode}_{exhibitCode}_{camId}_{datetime.now().date()}_heatMapImage.npy"
        if not os.path.exists(imageFileName):    
            heatmap_accumulator = np.zeros((frameHeight, frameWidth), dtype=np.float32)
        else:
            heatmap_accumulator = np.load(imageFileName)
        
        allPersonsPresent = {}
        allPeronPresentTime = {}
        idTimeMapping = {}
        
        startTime = fetchStartDate(rois, cameraInfo,abortInterval, prevFileName)
        timeDiff = (datetime.now() - startTime).total_seconds()/3600
        newFrame = None
        resultNewFrame = None
        while timeDiff > 0 and timeDiff < abortInterval:
            ret, frame = cap.read()
            if not ret:
                cap.release()
                cap = VideoCaptureBuffer(video)
                time.sleep(1)
                continue

            frame = cv2.resize(frame, (frameWidth, frameHeight))
            results = model.track(frame,imgsz = 640, conf=0.2,persist=True, iou = 0.4, tracker = "bytetrack.yaml", verbose =False)
            for roi in rois.keys():
                # newFrame = extractImage(frame.copy(), rois.get(roi))
                # cv2.imwrite(f"masked_{roi}.jpg", newFrame)
                if allPersonsPresent.get(roi) is None:
                    allPersonsPresent.update({roi:{}})

                if allPeronPresentTime.get(roi) is None:
                    allPeronPresentTime.update({roi:{}})
                
                if idTimeMapping.get(roi) is None:
                    idTimeMapping.update({roi:{}})


                new_person_detected_cordinates = []
                personIds = []

                for result in results:
                    resultFrame = result.orig_img.copy()
                    for box in result.boxes:
                        classId = box.cls[0]
                        
                        if int(classId) == 0 and box.id is not None: 
                            x, y, w, h = map(int, box.xywh[0])
                            if personInsidePolygon(rois.get(roi), (x,y)):
                                id = box.id[0].item()
                                personIds.append(id)
                                frame = cv2.circle(frame, (x,y), 6,thickness=1, color=(0,0,255))
                                frame = cv2.putText(frame, f"id: {id} {roi}", (int(x), int(y)), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
                                
                                if id not in allPersonsPresent.get(roi):
                                    allPersonsPresent.get(roi).update({id: time.time()})
                                    idTimeMapping.get(roi).update({id: time.time()})
                                    new_person_detected_cordinates.append((x,y))
                                
                                # prevTime = fetchPrevTime(fileName, id)
                                
                                time_entry =  allPeronPresentTime.get(roi, {})
                                prevTime =  time_entry.get(id, 0)
                                time_entry[id] = prevTime + abs(time.time()- allPersonsPresent.get(roi).get(id))
                                allPersonsPresent.get(roi).update({id: time.time()})
                                saveDataInFile(fileName, time_entry[id], int(idTimeMapping.get(roi).get(id)), roi)

                heatMap = drawHeatMap(new_person_detected_cordinates, heatmap_accumulator, frame)
                removePersonId(allPersonsPresent.get(roi), personIds)
            # out.write(frame)
            overLapImg = megreHeatMapWithOrginalImage(frame, heatMap, rois)
            cv2.imshow("Crowd Heatmap", overLapImg)
            #save a frame every 5 seconds 
            resultNewFrame = overLapImg.copy()
            timeDiff = (datetime.now() - startTime).total_seconds()/3600  
            currentTime = int(time.time())
            # if abs(lastUpdatedTime - currentTime) % updateFaceInterval == 0:
            if currentTime - lastUpdatedTime >= updateFaceInterval:
                threading.Thread(target = updateImage, args=(ftp, resultNewFrame, compCode, boothCode , exhibitCode, camId, heatmap_accumulator)).start()
                # updateImage(ftp, frame, compCode, boothCode , exhibitCode, camId, heatmap_accumulator)
                lastUpdatedTime = currentTime
            if cv2.waitKey(1) & 0xFF == ord('q'):  # Press 'q' to exit
                break
        updateImage(ftp, resultNewFrame, compCode, boothCode , exhibitCode, camId, heatmap_accumulator, finalImage = resultNewFrame)
        saveDataInDB(fileName, rois, camId)
        
#erase this
        for roi in rois.keys():
            print(f"No of people in {roi}: {len(allPeronPresentTime.get(roi, {}))}")
        for roi in rois.keys():
            data = {
                'Id': list(allPeronPresentTime.get(roi, {}).keys()), 
                'Time': list(allPeronPresentTime.get(roi, {}).values())
            }
            df = pd.DataFrame(data)
            
            df.to_excel(f"{roi}.xlsx", sheet_name="Sheet1", index=False)

        cap.release()
        # out.release()
        cv2.destroyAllWindows()
    except Exception as e:
        logging.error(f"Error in crowdHeatMap IN {camId}: {e}\n{traceback.format_exc()}")

# if __name__ == "__main__": 
#     try:
#         # video =  config["Pose-Estimation"]["rtsp_url"]
#         video = "staffabsentmain2.mp4"
#         # video = "rtsp://ttl:transline321@192.168.10.160:554/Streaming/Channels/101"
#         if not video:
#             logging.error("Error Video not found in config.ini")
#             print("Error Video not found in config.ini")
#         crowdHeatMap(video)
#     except Exception as e:
#         logging.error(f"Error in main: {e}")
#         print(f"Error in main: {e}")
