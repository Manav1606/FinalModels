import cv2
from ultralytics import YOLO
import time
import threading
import os
import logging
from datetime import datetime

if not os.path.exists(os.path.join(os.getcwd(), f"heatMap_{datetime.now().date()}.log")):
    logging.basicConfig(
        filename=f"Detect_Faces_{datetime.now().date()}.log", 
        level=logging.ERROR,  
        format='%(asctime)s - %(levelname)s - %(message)s',  
        filemode='w'  
    )
else:
    logging.basicConfig(
        filename=f"Detect_Faces_{datetime.now().date()}.log",
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='a'  # Append to the file when it exists
    )
        
def faces():
    try:
        model = YOLO("yolov8n-face.pt")
        input_folder = "ctbkarolbagh"
        output_folder = "DetectedFaces"
        os.makedirs(output_folder, exist_ok=True)
        for fileName in os.listdir(input_folder):
            if fileName.lower().endswith(".jpg"):
                img_path = os.path.join(input_folder, fileName)
                img = cv2.imread(img_path)
                # img = cv2.resize(img, (2528, 1408))
                height, width = img.shape[:2]
                
                if img is None:
                    logging.error(f"Error loading image: {img_path}")
                    continue

                # img = cv2.resize(img, (960, 640))
                try:
                    results = model.predict(source=img, imgsz= 640, conf=0.5, device="cpu", verbose=True )
                except Exception as e:
                    logging.error(f"Error in model prediction: {e}")
                    continue

                for result_idx, result in enumerate(results):
                    for box_idx, box in enumerate(result.boxes):
                        try:
                            x, y, w, h = map(int, box.xywh[0])
                        except Exception as e:
                            logging.error(f"Error extracting box coordinates: {e}")
                            continue


                        x1 = max(0, int(x - 2*w))
                        y1 = max(0, int(y - 2*h))
                        x2 = min(width, int(x + 2*w))
                        y2 = min(height, int(y + 2*h))

                        face = img[y1:y2, x1:x2]
                        frameConventions = fileName.split("_")

                        if face.size > 0:  # Make sure the region is valid
                            frameName = f"{frameConventions[0]}_{frameConventions[1]}_{frameConventions[2]}_{frameConventions[3][:-2]}{box_idx:02}_{frameConventions[4]}_{frameConventions[5]}_{frameConventions[6]}.jpg"
                            save_path = os.path.join(output_folder, frameName)
                            newFace = cv2.resize(face, (256, 256))
                            success = cv2.imwrite(save_path, newFace)
                            if not success:
                                logging.error(f"Error saving image: {save_path}")  
                    # cv2.waitKey(500)
        cv2.destroyAllWindows()
    except Exception as e:
        logging.error(f"Error in faces function: {e}")

if __name__ == "__main__":
    faces()
      
