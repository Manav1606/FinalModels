import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import threading
from pathlib import Path
import json
import os
import logging
import queue
import time 

log_filename = f"sackMqttt.log"
log_filepath = os.path.join(os.getcwd(), log_filename)
logger = logging.getLogger('sackBMqtt_logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    file_mode = 'a' if os.path.exists(log_filepath) else 'w'
    file_handler = logging.FileHandler(log_filepath, mode=file_mode)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

queue_data = queue.Queue()

class MQTTClient:
    def __init__(self, client_id, broker='localhost', port=1883, keepalive=60, topic = None, on_message=None):
        self.client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311, transport="tcp", userdata=None, callback_api_version=CallbackAPIVersion.VERSION2)
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
        try:
            print(f"Connecting to {self.broker}:{self.port}")
            self.client.connect(self.broker, self.port, self.keepalive)
        except Exception as e:
            logger.error(f"Error connecting to MQTT broker: {e}")

    # Start the client loop
    def loop_forever(self):
        try:
            self.client.loop_forever()
        except Exception as e:
            logger.error(f"Error in MQTT loop: {e}")

    def loop_start(self):
        try:
            print("Starting MQTT client loop")
            self.client.loop_start()
        except Exception as e: 
            logger.error(f"Error starting MQTT loop: {e}")

    def loop_stop(self):
        try:
            self.client.loop_stop()
        except Exception as e:
            logger.error(f"Error stopping MQTT loop: {e}")

    # Subscribe to a topic
    def subscribe(self, topic, qos=0):
        try:
            with self.lock:
                print(f"Subscribing to topic: {topic}")
                self.client.subscribe(topic, qos)
        except Exception as e:
            logger.error(f"Error subscribing to topic {topic}: {e}")

    # Publish a message
    def publish(self, topic, payload, qos=0, retain=False):
        try:
            with self.lock:
                print(f"Publishing to {topic}: {payload}")
                self.client.publish(topic, payload, qos, retain, properties=None)
        except Exception as e:
            logger.error(f"Error publishing to topic {topic}: {e}")

    # Default callback for successful connection
    def on_connect(self,client, userdata, flags, reason_code, properties =None):
        try:
            print(f"Connected with result code: {reason_code}")
            if reason_code == 0:
                self.subscribe(self.topic,0) if self.topic else None
        except Exception as e:
            logger.error(f"Error in on_connect: {e}")

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
            return
        
        if not data:
            return
        
        file_path = Path(fileName)
        temp_path = file_path.with_suffix('.tmp')
        while True:
            try:
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    f.flush()
                    os.fsync(f.fileno())
                
                os.replace(temp_path, file_path)
                break
            except Exception as e:
                continue
        return
    except Exception as e:
        return
    
def on_message(client, userdata, message):
    try:
    # queue_data.put(message.payload.decode('utf-8'))
        print(f"Received message: {message.payload.decode('utf-8')} on topic: {message.topic}")
        message = json.loads(message.payload.decode('utf-8'))
        if message:
            filePath = "C://Users//manav//Downloads//heatMap//Sack-Bag-Count//mqtt.json"
            if not os.path.exists(filePath):
                with open(filePath, "w") as f:
                    json.dump({"start": [], "stop": []}, f, indent=4)
            with open(filePath, "r") as f:
                data = json.load(f)
                
            data.setdefault("start", [])
            data.setdefault("stop", [])
            
            if message.get("status") == "start":
                data["start"].append(message) 
            else:
                data["stop"].append(message)
            saveDataInJson(filePath, data)   
    except Exception as e:
        logger.error(f"Error in on_message: {e}")
        return
            

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = MQTTClient(client_id="test_client", topic="sack/bag/status", on_message=on_message)
    
    client.connect()
    client.loop_start()
    
    # Keep the script running to listen for messages
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        client.loop_stop()
        print("Exiting...")