import cv2
import numpy as np
import onnxruntime as ort
import os
import logging
from datetime import datetime

onnx_model_path = "yolov8n-face.onnx" 
options = ort.SessionOptions()
options.intra_op_num_threads = 2  
options.inter_op_num_threads = 1 
session = ort.InferenceSession(onnx_model_path, providers=['CPUExecutionProvider'], sess_options=options)
input_name = session.get_inputs()[0].name
input_shape = session.get_inputs()[0].shape 
 
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

def preprocess(img, input_size=(640, 640)):
    try:
        img_resized = cv2.resize(img, input_size)
        img_rgb = cv2.cvtColor(img_resized, cv2.COLOR_BGR2RGB)
        img_transposed = np.transpose(img_rgb, (2, 0, 1))  
        img_normalized = img_transposed / 255.0
        img_input = img_normalized.reshape(1, 3, 640, 640).astype(np.float32) 
        return img_input, img.shape[1], img.shape[0]
    except Exception as e:
        logging.error(f"Error in preprocessing image: {e}")
        return None, None, None



def postprocess(output, conf_threshold=0.5, orig_w=640, orig_h=640, input_size=(640, 640)):
    try:
        boxes = []
        results = output[0].transpose()
        for det in results: 
            x1, y1, x2, y2, conf = det
            if conf > conf_threshold:
                # scale to original size
                x1 = int((x1 / input_size[0]) * orig_w)
                y1 = int((y1 / input_size[1]) * orig_h)
                x2 = int((x2 / input_size[0]) * orig_w)
                y2 = int((y2 / input_size[1]) * orig_h)
                x1 = x1 - 1.5*x2
                y1 = y1 - 1.5*y2
                x2 = x1 + 2.5*x2
                y2 = y1 + 2.5*y2
                boxes.append((x1, y1, x2, y2))
        return boxes
    except Exception as e:
        logging.error(f"Error in postprocessing output: {e}")
        return []

def detectFaces():
    try:
        # net = cv2.dnn.readNetFromONNX('yolov8n-face.onnx')
        input_folder = "ctbkarolbagh"
        output_folder = "DetectedFaces"
        os.makedirs(output_folder, exist_ok=True)
        
        for fileName in os.listdir(input_folder):
            if fileName.lower().endswith(".jpg"):
                img_path = os.path.join(input_folder, fileName)
                img = cv2.imread(img_path)
        
                input_tensor, orig_w, orig_h = preprocess(img)
                outputs = session.run(None, {input_name: input_tensor})

                boxes = postprocess(outputs[0], conf_threshold=0.3, orig_w=orig_w, orig_h=orig_h)
                
                j = 0
                for i, (x1, y1, x2, y2) in enumerate(boxes):
                    x1, y1, x2, y2 = map(int, [x1, y1, x2, y2])
                    # cv2.rectangle(img, (x1, y1), (x2, y2), (0, 255, 0), 2)
                    frameConventions = fileName.split("_")
                    face_crop = img[y1:y2, x1:x2]
                    if face_crop.size > 0:
                        j = j + 1
                        frameName = f"{frameConventions[0]}_{frameConventions[1]}_{frameConventions[2]}_{frameConventions[3]}{j:02}_{frameConventions[4]}_{frameConventions[5]}_{frameConventions[6]}.jpg"
                        save_path = os.path.join(output_folder, frameName)  
                        cv2.imwrite(save_path, face_crop)

        cv2.destroyAllWindows()
    except Exception as e:
        logging.error(f"Error in face detection: {e}")
    
if __name__ == "__main__": 
    detectFaces()