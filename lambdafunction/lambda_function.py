import json
import boto3
import pymysql


# Initialize AWS clients
s3_client = boto3.client('s3')

# Constants
MEDIA_BUCKET_NAME = "car-network-media-bucket"
DB_HOST = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
DB_USER = 'admin'
DB_PASSWORD = 'FrostGaming1!'


def lambda_handler(event, context):
    # Determine the HTTP method
    http_method = None
    http_method = event['httpMethod']
    
    
    if http_method == 'GET':
        pass
    
    elif http_method == 'POST':
        try:
            # Parse the request body
            request_body = json.loads(event['body'])
            user_id = request_body.get('user_id')
            post_text = request_body.get('post_text')
            media_filename = request_body.get('media_filename')

            # Validate input
            if not user_id or not post_text:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing required parameters'})
                }

            
            # Save post data to the database
            post_id = save_post_to_database(user_id, post_text)
            
            # Generate a pre-signed URL for the media upload if media exists

            presigned_url = None

            if media_filename:
                media_key = f'uploads/{user_id}/{media_filename}'
                presigned_url = generate_presigned_url(MEDIA_BUCKET_NAME, media_key)
            
                if not presigned_url:
                    raise Exception("Failed to generate pre-signed URL")
            
            return {
                'statusCode': 200,
                'body': json.dumps({'post_id': post_id, 'upload_url': presigned_url})
            }
    
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)})
            }
    
    else:
        return {
            'statusCode': 400,
            'body': json.dumps(event)
        }
    
    

def generate_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a pre-signed URL for uploading to S3."""
    try:
        response = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket_name, 'Key': object_name},
            ExpiresIn=expiration
        )
        return response
    except Exception as e:
        print(f"Error generating pre-signed URL: {e}")
        return None

def save_post_to_database(user_id, post_text):
    """Save post data to the MySQL database."""
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database="post_db")
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO posts (user_id, content) VALUES (%s, %s)"
            cursor.execute(sql, ( user_id, post_text))
            post_id = cursor.lastrowid
        connection.commit()

        return post_id
    
    except Exception as e:
        connection.rollback()
        print(str(e))
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    finally:
        connection.close()

# def save_post_media_to_database(user_id, post_id, media_url):
#     """
#     Save medias for post
#     """
#     connection = pymysql.connect(host=DB_HOST,
#                                  user=DB_USER,
#                                  password=DB_PASSWORD,
#                                  database="post_db")
#     try:
#         with connection.cursor() as cursor:
#             sql = "INSERT INTO post_media (user_id, post_id, media_url) VALUES (%s, %s, %s)"
#             cursor.execute(sql, (user_id, post_id, media_url))
#         connection.commit()
#     except Exception as e:
#         connection.rollback()
#         return {
#             'statusCode': 500,
#             'body': json.dumps(f'Error: {str(e)}')
#         }
#     finally:
#         connection.close()

# def save_metadata_to_database(post_id, user_id, media_key):
#     """Save media metadata to the MySQL database."""
#     connection = pymysql.connect(host=DB_HOST,
#                                  user=DB_USER,
#                                  password=DB_PASSWORD,
#                                  database="media_metadata_db")
#     try:
#         with connection.cursor() as cursor:
#             sql = "INSERT INTO media_metadata (post_id, user_id, media_key) VALUES (%s, %s, %s)"
#             cursor.execute(sql, (post_id, user_id, media_key))
#         connection.commit()
#     except Exception as e:
#         connection.rollback()
#         return {
#             'statusCode': 500,
#             'body': json.dumps(f'Error: {str(e)}')
#         }
#     finally:
#         connection.close()


