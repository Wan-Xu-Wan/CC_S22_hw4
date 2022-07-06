import json
import boto3
import requests
from boto3.dynamodb.conditions import Key
import uuid
import random
import logging
from datetime import datetime
import logging

host = 'https://search-ccs22-ikx7naonkhx3bqbsffouzpa3aq.us-east-1.es.amazonaws.com/'
region = 'us-east-1' 
service = 'es'
index = "posts"
url = host  + index + '/_search'

def lambda_handler(event, context):
    # TODO implement
    tagOne = event["currentIntent"]["slots"]["tagOne"]
    tagTwo = event["currentIntent"]["slots"]["tagTwo"]

    print(tagOne)
    
    print(tagTwo)
    
    headers = { "Content-Type": "application/json" }
    
    query1 = {
        "size": 25,
        "query": {
            "multi_match": {
                "query": tagOne,
                "fields": ["tags"]
            }
        }
    }
    r1 = requests.get(url, auth=("Test1", "Test1Test1!"), headers=headers, data=json.dumps(query1))
    rdata1 = json.loads(r1.text)
    result1 = rdata1['hits']['hits']
    
    ridlist1 = []
    
    for row in result1:
        rid = row['_id']
        ridlist1.append(rid)
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('posts')
    
    plist = []
    
    for i in ridlist1:
        if len(plist) == 3: break
        resp1 = table.query(KeyConditionExpression=Key('id').eq(i))
        if resp1['Items']: plist.append(resp1['Items'])
        
    if tagTwo is not None and tagTwo != "NA":
        
        query2 = {
            "size": 25,
            "query": {
                "multi_match": {
                    "query": tagTwo,
                    "fields": ["tags"]
                }
            }
        }
        r2 = requests.get(url, auth=("Test1", "Test1Test1!"), headers=headers, data=json.dumps(query2))
        rdata2 = json.loads(r2.text)
        result2 = rdata2['hits']['hits']
        ridlist2 = []
        for row in result2:
            rid = row['_id']
            ridlist2.append(rid)
            
        for i in ridlist2:
            if len(plist) == 6: break
            resp2 = table.query(KeyConditionExpression=Key('id').eq(i))
            if resp2['Items']: plist.append(resp2['Items'])


    logging.basicConfig(format="[%(levelname)s] [%(name)s] [%(asctime)s]: %(message)s",
                        level="INFO")
    logger = logging.getLogger(__name__)
    
    sns = boto3.client('sns', region_name='us-east-1')
    sns.publish(TopicArn="arn:aws:sns:us-east-1:987936919727:postTopic",
                Message= json.dumps(plist))
    response = {
        "sessionAttributes": {},
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": 'Fulfilled',
            "message": {
                'contentType': 'PlainText',
                'content': 'Thanks, the posts with tags {} and {} have been sent to your mailbox'.format(tagOne, tagTwo)
            }
        }
    }
        
    
    return response
    
