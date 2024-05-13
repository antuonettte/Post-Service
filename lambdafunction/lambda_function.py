import json

def lambda_handler(event, context):
    # Post Service
    return {
        'statusCode': 200,
        'body': json.dumps('Post Service')
    }
