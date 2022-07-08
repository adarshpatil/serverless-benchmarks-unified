import csv
import json
import time

import nltk
nltk.data.path.append('nltk_data/')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

#import boto3

import pickle 

def readcsv():
    ### disaggr get begin
    reviews = open('data/real_reviews.csv')
    reviews_csv = csv.DictReader(reviews)
    ### disaggr get end
    
    ### compute begin
    #DictReader -> convert lines of CSV to OrderedDict
    start = time.time()
    body = []
    for row in reviews_csv:
        #return just the first loop (row) results!
        review = {}
        for k,v in row.items():
            review[k] = int(v) if k == 'reviewType' else v
        body.append(review)
    print(time.time() - start)
    ### compute end
                
    ### disaggr put begin
    event = {'statusCode':200, 'body':body}
    event_pickle = pickle.dumps(event)
    ### disaggr put end

    return event_pickle



def sentiment(event_pickle):
    ### disaggr get begin
    event = pickle.loads(event_pickle)
    ### disaggr get end
    
    ### compute begin
    start = time.time()
    sid = SentimentIntensityAnalyzer()
    feedback = event['body'][0]['feedback']
    scores = sid.polarity_scores(feedback)

    if scores['compound'] > 0:
        sentiment = 1
    elif scores['compound'] == 0:
        sentiment = 0
    else:
        sentiment = -1
    print (time.time() - start)
    ### compute end

    ### disaggr put begin
    response = {'statusCode' : 200,
                'body' : { 'sentiment': sentiment,
                'reviewType': event['body'][0]['reviewType'] + 0,
                'reviewID': (event['body'][0]['reviewID'] + '0')[:-1],
                'customerID': (event['body'][0]['customerID'] + '0')[:-1],
                'productID': (event['body'][0]['productID'] + '0')[:-1],
                'feedback': (event['body'][0]['feedback'] + '0')[:-1]},
                'others': event['body']}
    event_pickle = pickle.dumps(response)
    ### disaggr put end
    
    return event_pickle


def publishSNS(event_pickle):
    '''
    Sends notification of negative results from sentiment analysis via SNS
    '''
    ### disaggr get begin
    event = pickle.loads(event_pickle)    
    ### disaggr get end
    
    ### compute begin
    start = time.time()
    #construct message from input data
    TopicArn = 'arn:aws:sns:XXXXXXXXXXXXXXXX:my-SNS-topic',
    Subject = 'Negative Review Received',
    Message = 'Review (ID = %i) of %s (ID = %i) received with negative results from sentiment analysis. Feedback from Customer (ID = %i): "%s"' % (int(event['body']['reviewID']), event['body']['reviewType'], int(event['body']['productID']), int(event['body']['customerID']), event['body']['feedback'])
    #Not publishing to avoid network delays in experiments
    #sns = boto3.client('sns')
    #sns.publish(TopicArn, Subject, Message)
    print (time.time() - start)
    ### compute end
    
    ### disaggr put begin
    ### NO PUT ACTION
    ### disaggr put end

    #pass through values
    #print("publishing event")
    #print(event)
    #return event


def writetodb(event_pickle):
    ### disaggr get begin
    event = pickle.loads(event_pickle)    
    ### disaggr get end
    
    ### compute begin
    start = time.time()
    #dynamodb = boto3.client('dynamodb',aws_access_key_id="AKIAQ4WHHPCKGVH4HO6S",
    #                   aws_secret_access_key="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",
    #                   region_name="us-east-1")

    #select correct table based on input data
    if event['body']['reviewType'] == 0:
        tableName = 'faastlane-products-table'
    elif event['body']['reviewType'] == 1:
        tableName = 'faastlane-services-table'
    else:
        raise Exception("Input review is neither Product nor Service")
    #Not writing to table to avoid network delays in experiments
    response = {'statusCode':200, 'body': event['body']}
    print (time.time() - start)
    ### compute end
    
    ### disaggr put begin
    ### NO PUT ACTION
    ### disaggr put end
    
    #print("writing to db")
    #print(response)


#MAIN
parsedReviews = readcsv()

parsedReviews = sentiment(parsedReviews)

publishSNS(parsedReviews)

writetodb(parsedReviews)

#publish_out = publishSNS(parsedReviews)

#write_out = writetodb(parsedReviews)
print("Complete")
