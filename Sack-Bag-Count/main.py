import sackBagCount
import logging 
import os
import threading
import utilities

# make a logger file
log_filename = f"sackBagCount.log"
log_filepath = os.path.join(os.getcwd(), log_filename)
logger = logging.getLogger('sackBag_logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    file_mode = 'a' if os.path.exists(log_filepath) else 'w'
    file_handler = logging.FileHandler(log_filepath, mode=file_mode)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

#connect MQTT  
 
thr = {}
stopEvents = {}
def countSackBags():
    try:
        stopEvent = threading.Event()
        cameraId, bayNo, rtsp, direction, frameWidth, frameHeight, modelName, companyCode, storeCode = (
            "camera1", 1, "C:\\Users\\manav\\Downloads\\heatMap\\Sack-Bag-Count\\VID-20250530-WA0000.mp4", "left", 960, 640, "best.pt", "company123", "store456"
        )
        loi = [(437.5375061035156, 282.4874954223633), (797.5375061035156,  417.4874954223633)]
        roi = [{"x": 452.5375061035156, "y": 218.48749542236328}, {"x": 313.5375061035156, "y": 465.4874954223633}, {"x": 726.5375061035156, "y": 575.4874954223633}, {"x": 814.5375061035156, "y": 253.48749542236328}, {"x": 452.5375061035156, "y": 218.48749542236328}]
        t = threading.Thread(target=sackBagCount.sackBagCount, args=(cameraId, bayNo, rtsp, direction, frameWidth, frameHeight, modelName, companyCode, storeCode, stopEvent), kwargs={"client" : client, "loi": loi, "roi": roi})
        thr [bayNo] = t
        stopEvents[bayNo] = stopEvent
        t.start()
        
    except Exception as e:
        logger.error(f"Error in sackBagCount: {e}")
        return None
    
if __name__ == "__main__":
    client = utilities.MQTTClient(client_id="sack-bag-counter")
    client.connect()
    client.loop_start()  
    countSackBags()