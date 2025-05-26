import logging
from pathlib import Path
import time
import threading
import cv2
from ftplib import FTP, all_errors
import os
from io import BytesIO
import numpy as np

logger = logging.getLogger(__name__)

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
        


class setupFtp:
    def __init__(self, userName, password, host, port, timeout = 10):
        self.userName = userName
        self.password = password
        self.host = host
        self.port = port
        self.timeout = timeout
        self.ftp = None
        self.connect()
        # self.ftp = FTP(host = self.host, user = self.userName, passwd = self.password, port = self.port)

            
    def connect(self):
        try:
            self.ftp = FTP()
            self.ftp.connect(self.host, self.port, timeout=self.timeout)
            self.ftp.login(self.userName, self.password)
            logger.info(f"Connected to FTP server {self.host}:{self.port}")
        except all_errors as e:
            logger.error(f"FTP connection failed: {e}")
            self.ftp = None
            
    def sendFile(self,fileName, stream):
        if not self.ftp:
            logger.error("FTP connection is not established.")
            return False
        try:
            res = self.ftp.storbinary(f'STOR {fileName}', stream)
            msg = 'Upload %s to FTP Server %s.'
            if res.startswith('226 Transfer complete'):
                logger.error(msg % ('success', self.host))
                return True
            else:
                logger.error(msg % ('falied', self.host)) 
                return False  
        except all_errors as e:
            logger.error(f"Error in sendFile: {e}")
            return False
        
    def ftp_mkdir_recursive(self, path):
        try:
            if path == '' or path == '.':
                return
            parent = os.path.dirname(path)
            if parent and parent != path:
                self.ftp_mkdir_recursive(parent)
            
            try:
                self.ftp.mkd(path)
            except Exception as e:
                if "file already exists" in str(e):
                    return
                if "closed by the remote host" in str (e):
                    return
                self.ftp_mkdir_recursive(os.path.dirname(path))
                self.ftp.mkd(path)
        except Exception as e:
            logger.error(f"file already exits, {e}")

        
    def close(self):
        if self.ftp:
            try:
                self.ftp.quit() 
                logger.info("FTP connection closed.")
            except all_errors as e:
                logger.error(f"Error closing FTP connection: {e}")
            finally:
                self.ftp = None
        else:
            logger.error("FTP connection is not established.")
            return 

def saveDataInTxt(fileName,data):

    if not fileName.endswith(".txt"):
        logger.error(f"file Name is not a json")
        return
    
    if not data:
        logger.error("data is emrpty")
        return
    
    file_path = Path(fileName)
    with open(file_path, 'w') as f:
            f.write(data)
    return

def uploadFileOnFtp(ftp,frame, ftpPath):
    try:
        success, encoded_image = cv2.imencode('.jpg', frame)
        if success:
            image_bytes = encoded_image.tobytes()
            stream = BytesIO(image_bytes)
            res = ftp.sendFile(ftpPath,stream)
            return res
    except Exception as e:
        logger.error(f"Error in uploadFileOnFtp: {e}")
        return False
    
def point_position(a, b, p):
    try:
    # a, b, p = (x, y)
        cross = (b[0] - a[0]) * (p[1] - a[1]) - (b[1] - a[1]) * (p[0] - a[0])
        if cross > 0:
            return "left"
        elif cross < 0:
            return "right"
        else:
            return "On the Line"
    except Exception as e:
        logger.error(f"Error in point_position: {e}")

def objectInsidePolygon(points, person):
    try:
        pts = np.array([[int(p["x"]), int(p["y"])] for p in points], dtype=np.int32)
        is_inside = cv2.pointPolygonTest(pts, person, False)
        if is_inside >= 0:
            return True
        return False
    except Exception as e:
        logger.error(f"Error in objectInsidePolygon: {e}")
        return False

def fetchObject(results, objects = [], roi = None):
    try:
        objectsCoordinates = {}
        for result in results:
            for j,box in enumerate(result.boxes):
                    classId = box.cls[0]
                    if classId in objects and box.id is not None:
                        x, y, w, h = map(int, box.xywh[0])
                        id = box.id
                        objectsCoordinates[classId] = {}
                        if roi is not None:
                            if objectInsidePolygon(roi, {"x": x, "y": y}):
                                objectsCoordinates[classId].update({id: (x, y)})
                        else:
                            objectsCoordinates[classId].update({id:(x,y)})
        return objectsCoordinates
    except Exception as e:
        logger.error(f"error in fetch Object {e}")
        return {}
    


            