import crowdMapSecond
import configparser
import os
import json
import threading
import time
config_path = os.path.join(os.getcwd(), "config.ini")
config =  configparser.ConfigParser()
config.read(config_path)

def heatMap():
    cameras = json.loads(config["Heat-Map"]["cameras_info"])
    while True:
        thr = []

        for cameraInfo in cameras:
            if cameraInfo:
                t = threading.Thread(target=crowdMapSecond.crowdHeatMap, args=(cameraInfo,))
                t.start()  
                thr.append(t)  
        for t in thr:
            t.join()

        time.sleep(1)  

    
if __name__ == "__main__":
    heatMap()