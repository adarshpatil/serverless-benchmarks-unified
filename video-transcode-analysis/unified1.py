import cv2
import random
import os
import shutil
import tempfile
import pickle

"""
This benchmark represents the "Split and transcode video using massive parallelization"
from Thomson Reuters that uses AWS Step Functions and AWS Lambda
https://aws.amazon.com/step-functions/use-cases/

To simulate a get of the video file from disaggr mem to local mem
we simply make a copy of the input file to /dev/shm, which is a RAM-backed filesystem
https://stackoverflow.com/questions/40290770/opencv-loading-video-in-the-memory
"""
def locateframe(video_path, num_segments):
    ### get begin
    tempdir = tempfile.TemporaryDirectory(dir='/dev/shm')
    copypath = os.path.join(tempdir.name, os.path.basename(video_path))
    shutil.copy(video_path, copypath)
    ### get end
    
    ### compute begin
    video = cv2.VideoCapture(video_path)
    frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
    video.release()
    
    keyframes = []
    for i in range(0,num_segments):
        keyframes.append(int(frames/num_segments)*(i+1))
    ### compute end
    
    ### put begin
    keyframes_pickle = pickle.dumps(keyframes)
    ### put end
    
    return keyframes


def split(video_path, keyframes):
    ### get begin
    tempdir = tempfile.TemporaryDirectory(dir='/dev/shm')
    copypath = os.path.join(tempdir.name, os.path.basename(video_path))
    shutil.copy(video_path, copypath)
    ### get end
    
    ### compute begin
    video = cv2.VideoCapture(video_path)

    width = int(video.get(3))
    height = int(video.get(4))
    fourcc = cv2.VideoWriter_fourcc(*'XVID')

    seg_id = 0
    frame_ctr = 0
    segments = []
    segments.append(video_path+"-seg"+str(seg_id)+".avi")
    out = cv2.VideoWriter(segments[seg_id], fourcc, 20.0, (width, height))
    
    while video.isOpened():
        ret, frame = video.read()

        if ret:
            out.write(frame)
            frame_ctr += 1
            if (frame_ctr == keyframes[seg_id]):
                out.release()
                seg_id += 1
                if(seg_id == len(keyframes)):
                    break
                segments.append(video_path+"-seg"+str(seg_id)+".avi")
                out = cv2.VideoWriter(segments[seg_id], fourcc, 20.0, (width, height))
        else:
            break
    
    video.release
    ### compute end
    
    ### put begin
    segments_pickle = pickle.dumps(segments)

    for seg in segments:
        tempdir = tempfile.TemporaryDirectory(dir='/dev/shm')
        copypath = os.path.join(tempdir.name, os.path.basename(seg))
        shutil.copy(seg, copypath)
    ### put end
    return segments


def process(seg_id, video_path, keyframes):
    ### get begin
    tempdir = tempfile.TemporaryDirectory(dir='/dev/shm')
    copypath = os.path.join(tempdir.name, os.path.basename(video_path))
    shutil.copy(video_path, copypath)
    ### get end
    
    ### compute begin
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
    ### compute end    
    
    ### put begin
    tempdir = tempfile.TemporaryDirectory(dir='/dev/shm')
    copypath = os.path.join(tempdir.name, os.path.basename(seg_out))
    shutil.copy(seg_out, copypath)
    ### put end
    
    # analyse should run video analysis for profanity sensor=1 else 0
    # instead we randomly decide if this segment should be part of concat
    #sensor_segment = random.randint(0,1)
    sensor_segment = 0
    return (seg_out,sensor_segment)

def validate(processed_segments):
    ### get begin
    # Nothing to read from disagg mem
    # processed_segments is sent from the previous function inline
    ### get end
    
    ### compute begin
    validated_seg = []
    for seg,sensor in processed_segments:
        if sensor == False:
            validated_seg.append(seg)
    ### compute end
    
    ### put begin
    validated_seg_pickle = pickle.dumps(validated_seg)
    ### put end
                
    return validated_seg


def concat(segment_metadata):
    ### get begin
    for s in segment_metadata:
        tempdir = tempfile.TemporaryDirectory(dir='/dev/shm')
        copypath = os.path.join(tempdir.name, os.path.basename(s))
        shutil.copy(s, copypath)
    ### get end
    
    ### compute begin
    if(len(segment_metadata)==0):
        return
    video = cv2.VideoCapture(segment_metadata[0])
    width = int(video.get(3))
    height = int(video.get(4))
    video.release()
    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    concat_video = "concat.avi"
    out = cv2.VideoWriter(concat_video, fourcc, 20.0, (width, height))
    for s in segment_metadata:
        video = cv2.VideoCapture(s)
        while video.isOpened():
            ret, frame = video.read()
            if ret:
                out.write(frame)
            else:
                break
        video.release()

    out.release()
    ### compute end
    
    ### put begin
    tempdir = tempfile.TemporaryDirectory(dir='/dev/shm')
    copypath = os.path.join(tempdir.name, os.path.basename(concat_video))
    shutil.copy(concat_video, copypath)    
    ### put end


##### MAIN #####
video_path = "data/big_buck_bunny_720p_2mb.mp4"
#video_path = "data/SampleVideo_1280x720_10mb.mp4"
num_segments = 2

segment_metadata = locateframe(video_path, num_segments)
segments = split(video_path, segment_metadata)

processed_segments = []
for i,s in enumerate(segments):
    processed_segments.append(process(i,s, segment_metadata))

segment_metadata = validate(processed_segments)

concat(segment_metadata)
