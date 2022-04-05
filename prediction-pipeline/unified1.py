import os
import json
import time

#import boto3
import numpy as np
from PIL import Image
import pickle
import tensorflow as tf

def resizeHandler():
    ### disaggr get begin
    image = Image.open("data/image.jpg")
    image.load()
    ### disaggr get end

    ### compute begin    
    img = np.array(image.resize((224, 224))).astype(np.float) / 128 - 1
    resize_img = img.reshape(1, 224,224, 3)
    ### compute end

    ### disaggr put begin
    resize_pickle = pickle.dumps(resize_img)
    ### disaggr put end

    return resize_pickle


def predict(resize_pickle):
    ### disaggr get begin
    img = pickle.loads(resize_pickle)
    model = open('data/mobilenet_v2_1.0_224_frozen.pb', 'rb').read()
    ### disaggr get end

    ### compute begin
    gd = tf.compat.v1.GraphDef.FromString(model)
    inp, predictions = tf.import_graph_def(gd,  return_elements = ['input:0', 'MobilenetV2/Predictions/Reshape_1:0'])
    with tf.compat.v1.Session(graph=inp.graph):
        x = predictions.eval(feed_dict={inp: img})
    ### compute end 

    ### disaggr put begin
    response = {"statusCode": 200, "body": json.dumps({'predictions': x.tolist()}) }
    response_pickle = pickle.dumps(response)
    ### disaggr put end

    return response_pickle

def render(event_pickle):
    ### disaggr get begin
    event = pickle.loads(event_pickle)
    ### disaggr get end

    ### compute begin
    body = json.loads(event['body'])
    x = np.array(body['predictions'])
    text = "Top 1 Prediction: " + str(x.argmax()) + str(x.max())
    ### compute end

    
    ### disaggr put begin
    response = {"statusCode": 200, "body": json.dumps({'render': text}) }
    response_pickle = pickle.dumps(response)
    ### disaggr put end

    return response_pickle


resize_img = resizeHandler()
predictions = predict(resize_img)
response = render(predictions)
