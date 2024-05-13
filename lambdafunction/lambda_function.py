import json

def lambda_handler(event, context):
    # Post Servicesad
    return {
        'statusCode': 200,
        'body': json.dumps('Post Service')
    }
