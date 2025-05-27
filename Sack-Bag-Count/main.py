import sackBagCount
import logging 
import os
import threading

log_filename = f"sackBagCount.log"
log_filepath = os.path.join(os.getcwd(), log_filename)
logger = logging.getLogger('dwellTime_logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    file_mode = 'a' if os.path.exists(log_filepath) else 'w'
    file_handler = logging.FileHandler(log_filepath, mode=file_mode)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
def countSackBags():
    try:
        cameraId, bayNo, rtsp, direction, frameWidth, frameHeight, modelName, companyCode, storeCode = (
            "camera1", 1, "rtsp://example.com/stream", "left", 960, 640, "best.pt", "company123", "store456"
        )
        threading.Thread(target=sackBagCount.sackBagCount, args=(cameraId, bayNo, rtsp, direction, frameWidth, frameHeight, modelName, companyCode, storeCode)).start()
    except Exception as e:
        logger.error(f"Error in sackBagCount: {e}")
        return None
        
    