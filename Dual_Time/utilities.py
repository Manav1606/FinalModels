import os
import sqlite3
import time
import threading
import cv2
import numpy as np
from pathlib import Path
from ftplib import FTP
import json
import logging
import requests
from io import BytesIO

logger = logging.getLogger('dualTime_logger')

class setupDB:
    def __init__(self, table):
        db_path = os.path.join(os.path.dirname(__file__), 'myDatabase.db')
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute(table)
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
            print("beforeFileName", fileName)
            fileName = fileName.replace('\\', '/')
            print("afterFileName", fileName)
            self.ensure_ftp_path(fileName)
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
        
    def ensure_ftp_path(self, path):
        try:
            self.ftp.cwd("/")
            parts = path.strip('/').split('/')
            current_path = ''
            print("parts", parts)
            for part in parts:
                print('hii')
                current_path = f"{current_path}/{part}" if current_path else part
                
                try:
                    self.ftp.cwd(current_path)
                except Exception:
                    self.ftp.mkd(current_path)
                    self.ftp.cwd(current_path)
        except Exception as e:
            logger.error(f"error in ensure_ftp_path {e}")  

        
    def close(self):
        if self.ftp:
            try:
                self.ftp.quit() 
                logging.info("FTP connection closed.")
            except Exception as e:
                logging.error(f"Error closing FTP connection: {e}")
        else:
            logging.error("FTP connection is not established.")
            return 
        
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
        
def personInsidePolygon(points, person):
    pts = np.array([[int(p["x"]), int(p["y"])] for p in points], dtype=np.int32)
    is_inside = cv2.pointPolygonTest(pts, person, False)
    if is_inside >= 0:
        return True
    return False
   
def saveDataInFile(fileName, data, id, roi):
    try:
        if not fileName.endswith(".json"):
            logging.error("File Name is not json")
            return
        if not data:
            logging.error("Data is empty")
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
            return False
    except Exception as e:
        logging.error(f"Error in sendRequest: {e}")
        return False
    
def uploadFileOnFtp(ftp,frame, ftpPath):
    try:
        success, encoded_image = cv2.imencode('.jpg', frame)
        if success:
            image_bytes = encoded_image.tobytes()
            stream = BytesIO(image_bytes)
            res = ftp.sendFile(ftpPath,stream)
            return res
    except Exception as e:
        logging.error(f"Error in uploadFileOnFtp: {e}")
        return False
