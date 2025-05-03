import cv2
from ultralytics import YOLO
import time
import threading
import os

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

        
def faces():
    # cap = VideoCaptureBuffer(video)
    # face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
    # face_detector = dlib.get_frontal_face_detector()
    try:
        model = YOLO("yolov8n-face.pt")
        # print("ccccc ", os.listdir("ctbkarolbagh"))
        input_folder = "ctbkarolbagh"
        output_folder = "DetectedFaces"
        os.makedirs(output_folder, exist_ok=True)
        i=0
        for fileName in os.listdir(input_folder):
            i = i + 1 
            if fileName.lower().endswith(".jpg"):
                img_path = os.path.join(input_folder, fileName)
                img = cv2.imread(img_path)
                # img = cv2.resize(img, (2528, 1408))
                height, width = img.shape[:2]
                
                if img is None:
                    print("Error loading image:", img_path)
                    continue

                # img = cv2.resize(img, (960, 640))
                try:
                    results = model.predict(source=img, imgsz= 640, conf=0.5, device="cpu", verbose=True )
                except Exception as e:
                    print("Error in model prediction:", e)
                    continue

                for result_idx, result in enumerate(results):
                    for box_idx, box in enumerate(result.boxes):
                        try:
                            x, y, w, h = map(int, box.xywh[0])
                        except Exception as e:
                            print("Error in box coordinates:", e)
                            continue


                        # Convert YOLO bbox to (x1, y1, x2, y2)
                        x1 = max(0, int(x - 2*w))
                        y1 = max(0, int(y - 2*h))
                        x2 = min(width, int(x + 2*w))
                        y2 = min(height, int(y + 2*h))

                        face = img[y1:y2, x1:x2]

                        if face.size > 0:  # Make sure the region is valid
                            save_path = os.path.join(output_folder, f"{fileName[:-4]}_face{box_idx}.jpg")
                            newFace = cv2.resize(face, (256, 256))
                            success = cv2.imwrite(save_path, newFace)
                            if not success:
                                print(f"Failed to save face image: {save_path}")   
                    # cv2.waitKey(500)
        print("Toal faces detected: ", i)
        cv2.destroyAllWindows()
    except Exception as e:
        print("Error in face detection:", e)

    # while True:
    #     ret, frame = cap.read()
    #     if not ret:
    #         cap.release()
    #         cap = VideoCaptureBuffer(video)
    #         time.sleep(1)
    #         continue

    #     frame = cv2.resize(frame, (960, 640))
    #     newImg= frame.copy()
    #     # faces = face_detector(newImg)
    #     result = model.predict(source=newImg, conf=0.3, device= "cpu")
    #     # print("faces", faces)
    #     # for face in faces:
    #     #     x1 = face.left()
    #     #     y1 = face.top()
    #     #     x2 = face.right()
    #     #     y2 = face.bottom()
            
    #     #     cv2.rectangle(newImg, (x1, y1), (x2, y2), (255, 0, 0), 2)
    #     cv2.imshow('Detected Faces', result[0].plot())
    #     if cv2.waitKey(1) & 0xFF == ord('q'):  # Press 'q' to exit
    #             break
    # cap.release()
    # cv2.waitKey(0)
    # cv2.destroyAllWindows()

if __name__ == "__main__":
    faces()
      
# # Load the pre-trained Haar cascade classifier for face detection
# face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
# face_detector = dlib.get_frontal_face_detector()

# # Load an image
# img = cv2.imread('istockphoto-1368965646-612x612.jpg')
# gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
# # gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
# gray = cv2.equalizeHist(gray)
# # model = YOLO("yolov8n-face.pt")
# # result = model.predict(source=img, conf=0.7)
# # Detect faces
# # faces = face_cascade.detectMultiScale(gray, scaleFactor=1.05, minNeighbors=3)
# newImg= img.copy()
# faces = face_detector(newImg)
# print("imgaesss", face_detector(newImg))
# # Draw rectangles around faces
# for face in faces:
#     x1 = face.left()
#     y1 = face.top()
#     x2 = face.right()
#     y2 = face.bottom()
    
#     cv2.rectangle(newImg, (x1, y1), (x2, y2), (255, 0, 0), 2)


# # Show the result
# cv2.imshow('Detected Faces', newImg)
# # cv2.imshow('Detected Facess', result[0].plot())
# cv2.waitKey(0)
# cv2.destroyAllWindows()
