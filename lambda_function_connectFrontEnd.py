import json
import boto3
import requests


client = boto3.client('lex-runtime')

def lambda_handler(event, context):
    # TODO implement
    data_temp = json.dumps(event)
    
    data1 = json.loads(data_temp)
    
    qtext = event['queryStringParameters']['q']
    
    lex = client.post_text(
        botName='SearchPosts',
        botAlias='searchPosts',
        userId='test',
        sessionAttributes={},
        requestAttributes={},
        inputText=json.dumps(qtext),

    )
    response = {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(lex)
    }


    
    return response
