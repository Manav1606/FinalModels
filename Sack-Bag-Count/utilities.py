import logging
from pathlib import Path
import time
import threading
import cv2
from ftplib import FTP, all_errors
import os
from io import BytesIO
import numpy as np
import json
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import sqlite3
import requests


logger = logging.getLogger("sackBag_logger")

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
        
class setupDB:
    def __init__(self, table):
        db_path = os.path.join(os.path.dirname(__file__), 'sackBag.db')
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute(table)
        self.commit
    def commit(self):
        self.conn.commit()
    def close(self):
        self.conn.close()

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

class MQTTClient:
    def __init__(self, client_id, broker='localhost', port=1883, keepalive=60, topic = None, on_message=None, transport = "tcp"):
        self.client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311, transport=transport, userdata=None, callback_api_version=CallbackAPIVersion.VERSION2)
        self.broker = broker
        self.port = port
        self.keepalive = keepalive
        self.topic = topic
        self.lock = threading.Lock() 

        # Assign default callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = on_message if on_message else self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish

    # Connection to broker
    def connect(self):
        print(f"Connecting to {self.broker}:{self.port}")
        self.client.connect(self.broker, self.port, self.keepalive)

    # Start the client loop
    def loop_forever(self):
        self.client.loop_forever()

    def loop_start(self):
        self.client.loop_start()

    def loop_stop(self):
        self.client.loop_stop()

    # Subscribe to a topic
    def subscribe(self, topic, qos=0):
        with self.lock:
            print(f"Subscribing to topic: {topic}")
            self.client.subscribe(topic, qos)

    # Publish a message
    def publish(self, topic, payload, qos=0, retain=False):
        with self.lock:
            print(f"Publishing to {topic}: {payload}")
            self.client.publish(topic, payload, qos, retain, properties=None)

    # Default callback for successful connection
    def on_connect(self,client, userdata, flags, reason_code, properties =None):
        print(f"Connected with result code: {reason_code}")
        if reason_code == 0:
            self.subscribe(self.topic,0) if self.topic else None

    # Default callback for receiving a message
    def on_message(self, client, userdata, msg):
        print(f"Received message: '{msg.payload.decode()}' on topic: '{msg.topic}'")

    # Callback when disconnected
    def on_disconnect(self, client, userdata, DisconnectFlags, reason_code, properties =None):
        print(f"Disconnected with result code: {reason_code}")

    # Callback on subscribe
    def on_subscribe(self, client, userdata, mid, granted_qos, reason_code, properties =None):
        print(f"Subscribed with QoS: {granted_qos}")

    def on_publish(self, client, userdata, mid, reason_code, properties =None):
        print(f"Message published (mid: {mid})")

    def set_on_message(self, callback):
        self.client.on_message = callback

    def set_on_connect(self, callback):
        self.client.on_connect = callback


def saveDataInJson(fileName,data):
    try:

        if not fileName.endswith(".json"):
            logger.error(f"file Name is not a json")
            return
        
        if not data:
            logger.error("data is emrpty")
            return
        
        file_path = Path(fileName)
        temp_path = file_path.with_suffix('.tmp')
        
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            f.flush()
            os.fsync(f.fileno())
        
        os.replace(temp_path, file_path)
        return
    except Exception as e:
        logger.error(f"Error in saveDataInJson: {e}")
        return

def uploadFileOnFtp(ftp,frame, ftpPath):
    try:
        if ftp is None or not isinstance(ftp, setupFtp):
            logger.error("Invalid FTP connection")
            return False
        success, encoded_image = cv2.imencode('.jpg', frame)
        if success:
            image_bytes = encoded_image.tobytes()
            stream = BytesIO(image_bytes)
            res = ftp.sendFile(ftpPath,stream)
            return res
    except Exception as e:
        logger.error(f"Error in uploadFileOnFtp: {e}")
        return False

def sendRequest(url, data = None, method = "POST"):
    try:
        logger.info(f"data {data}")
        headers = {
            'content-type': 'application/json',
        }
        if method == "POST":
            response = requests.post(url, json=data, headers=headers)
        else:
            response = requests.get(url)
        if response.status_code == 200:
            logger.error(f"Data sent successfully to {url}")
            return {
                "data": response.json(),
                "status": 200
            }
        else:
            logger.error(f"Error in sendRequest: {response.status_code} - {response.text}")
            return{
                "data": response.json(),
                "status": response.status_code
            }
    except Exception as e:
        logger.error(f"Error in sendRequest: {e}")
        return {
            "status": 500
        }
    
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
                    classId = int(box.cls[0].item())
                    if classId in objects and box.id is not None:
                        x, y, w, h = map(int, box.xywh[0])
                        id = int(box.id.item())
                        if classId not in objectsCoordinates:
                            objectsCoordinates[classId] = {}
                        if roi is not None:
                            if objectInsidePolygon(roi, (x,  y)):
                                objectsCoordinates[classId][id] = (x, y)
                        else:
                            objectsCoordinates[classId][id] = (x, y)
        return objectsCoordinates
    except Exception as e:
        logger.error(f"error in fetch Object {e}")
        return {}
    


            