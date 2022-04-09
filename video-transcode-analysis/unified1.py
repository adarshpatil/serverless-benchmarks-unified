import cv2
import random

def locateframe(video_path, num_segments):
    video = cv2.VideoCapture(video_path)
    frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
    video.release()
    
    keyframes = []
    for i in range(0,num_segments):
        keyframes.append(int(frames/num_segments)*(i+1))
    print(keyframes)
    return keyframes


def split(video_path, keyframes):
    video = cv2.VideoCapture(video_path)

    width = int(video.get(3))
    height = int(video.get(4))
    fourcc = cv2.VideoWriter_fourcc(*'XVID')

    seg_id = 0
    frame_ctr = 0
    segments = []
    segments.append(video_path+"-seg"+str(seg_id)+".avi")
    out = cv2.VideoWriter(segments[seg_id], fourcc, 20.0, (width, height))
    
    print(len(keyframes))
    while video.isOpened():
        ret, frame = video.read()

        if ret:
            out.write(frame)
            frame_ctr += 1
            if (frame_ctr == keyframes[seg_id]):
                out.release()
                seg_id += 1
                print(frame_ctr)
                print(seg_id)
                if(seg_id == len(keyframes)):
                    break
                segments.append(video_path+"-seg"+str(seg_id)+".avi")
                out = cv2.VideoWriter(segments[seg_id], fourcc, 20.0, (width, height))
        else:
            break
    
    video.release
    print(segments)
    return segments


def process(seg_id, video_path):
    video = cv2.VideoCapture(video_path)
    
    width = int(video.get(3))
    height = int(video.get(4))
    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    seg_out = video_path+"-process"+str(seg_id)+".avi"
    out = cv2.VideoWriter(seg_out, fourcc, 20.0, (width, height), False)
    
    while video.isOpened():
        ret, frame = video.read()
        if ret:
            gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            out.write(gray_frame)
        else:
            break 
    
    out.release
    video.release
    
    #randomly decide if this segment should be part of concat
    #sensor_segment = random.randint(0,1)
    sensor_segment = 0
    return (seg_out,sensor_segment)

def validate(processed_segments):
    validated_seg = []
    for seg,sensor in processed_segments:
        if sensor == False:
            validated_seg.append(seg)
    return validated_seg


def concat(validated_seg):
    if(len(validated_seg)==0):
        return
    video = cv2.VideoCapture(validated_seg[0])
    width = int(video.get(3))
    height = int(video.get(4))
    video.release()
    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    out = cv2.VideoWriter("concat.avi", fourcc, 20.0, (width, height))
    for s in validated_seg:
        video = cv2.VideoCapture(s)
        while video.isOpened():
            ret, frame = video.read()
            if ret:
                out.write(frame)
            else:
                break
        video.release()

    out.release()
    

video_path = "data/big_buck_bunny_720p_2mb.mp4"
#video_path = "data/SampleVideo_1280x720_10mb.mp4"
num_segments = 2

keyframes = locateframe(video_path, num_segments)
segments = split(video_path, keyframes)

processed_segments = []
for i,s in enumerate(segments):
    processed_segments.append(process(i,s))

validated_seg = validate(processed_segments)
print(validated_seg)
concat(validated_seg)
