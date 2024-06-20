import json
import boto3
import pymysql
import logging


# Initialize AWS clients
s3_client = boto3.client('s3')

# Constants
MEDIA_BUCKET_NAME = "car-network-media-bucket"
DB_HOST = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
DB_USER = 'admin'
DB_PASSWORD = 'FrostGaming1!'
DB_NAME = "post_db"


#Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        http_method = event['httpMethod']
        path = event['resource']
        query_parameters = event.get('queryStringParameters', {})
        logger.info(event)
        logger.info(query_parameters)
        
        if http_method == 'GET':
            logger.info("Get Method Called on: %s", path)
        
            if path == '/post-management/posts':
                if query_parameters and 'user_id' in query_parameters:
                    return get_all_posts_by_user_id(event)
                else:
                    return {
                        'statusCode':400,
                        'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                        'body':json.dumps({'error': 'Missing user ID'})
                    }
            elif path == '/post-management/post':
                if query_parameters and  'id' in query_parameters:
                    return get_post_by_post_id(event)
                else:
                    return {
                        'statusCode':400,
                        'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                
                        'body':json.dumps({'error': 'Missing post ID'})
                    }
        elif http_method == 'POST':
            if path == '/post-management/like':
                user_id = query_parameters.get('user_id')
                post_id = query_parameters.get('post_id')
                return like_post(user_id, post_id)
            elif path == '/post-management/dislike':
                user_id = query_parameters.get('user_id')
                post_id = query_parameters.get('post_id')
                return dislike_post(user_id, post_id)
            return handle_post(event)
            
        elif http_method == "DELETE":
            if path == '/post-management/like':
                user_id = query_parameters.get('user_id')
                post_id = query_parameters.get('post_id')
                return unlike_post(user_id, post_id)
            if path == '/post-management/dislike':
                user_id = query_parameters.get('user_id')
                post_id = query_parameters.get('post_id')
                return delete_dislike(user_id, post_id)
            
        else:
            return {
                'statusCode': 405,'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                'body': json.dumps({'error': 'Method Not Allowed'})
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'error': str(e)})
        }
        
def like_post(user_id, post_id):
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    
    try:
        with connection.cursor() as cursor:
            # Check if post is already liked
            check_like_sql = "SELECT COUNT(*) FROM likes WHERE user_id = %s AND post_id = %s"
            cursor.execute(check_like_sql, (user_id, post_id))
            already_liked = cursor.fetchone()[0] > 0
            
            # Check if post is disliked
            check_dislike_sql = "SELECT COUNT(*) FROM dislikes WHERE user_id = %s AND post_id = %s"
            cursor.execute(check_dislike_sql, (user_id, post_id))
            already_disliked = cursor.fetchone()[0] > 0
            
            if already_liked:
                # If already liked, remove the like
                delete_like_sql = "DELETE FROM likes WHERE user_id = %s AND post_id = %s"
                cursor.execute(delete_like_sql, (user_id, post_id))
                decrement_like_sql = "UPDATE posts SET like_count = COALESCE(like_count, 0) - 1 WHERE id = %s"
                logger.info("decrementing like count")
                cursor.execute(decrement_like_sql, (post_id,))
                logger.info(cursor.fetchall())
            else:
                # Add like
                insert_like_sql = "INSERT INTO likes (user_id, post_id) VALUES (%s, %s)"
                cursor.execute(insert_like_sql, (user_id, post_id))
                increment_like_sql = "UPDATE posts SET like_count = COALESCE(like_count, 0) + 1 WHERE id = %s"
                cursor.execute(increment_like_sql, (post_id,))

                if already_disliked:
                    # If disliked, remove the dislike
                    delete_dislike_sql = "DELETE FROM dislikes WHERE user_id = %s AND post_id = %s"
                    cursor.execute(delete_dislike_sql, (user_id, post_id))
                    decrement_dislike_sql = "UPDATE posts SET dislike_count = COALESCE(dislike_count, 0) - 1 WHERE id = %s"
                    cursor.execute(decrement_dislike_sql, (post_id,))
            
            connection.commit()

        return {
            'statusCode': 201,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'message': 'Post like status updated successfully'})
        }

    except Exception as e:
        connection.rollback()
        logger.error(f"Error liking post: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'error': 'Failed to update like status'})
        }

    finally:
        connection.close()
    
def unlike_post(user_id, post_id):
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    
    try:
        logger.info("Unliking post")
        
        with connection.cursor() as cursor:
            # Delete from likes table
            delete_like_sql = """
            DELETE FROM likes
            WHERE user_id = %s AND post_id = %s
            """
            cursor.execute(delete_like_sql, (user_id, post_id))
            
            # Decrement like count on post
            decrement_like_sql = """
            UPDATE posts
            SET like_count = like_count - 1
            WHERE id = %s
            """
            cursor.execute(decrement_like_sql, (post_id,))
            
            connection.commit()
        
        return {
            'statusCode': 200,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'message': 'Post unliked successfully'})
        }
    
    except Exception as e:
        connection.rollback()
        logger.error(f"Error unliking post: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'error': 'Failed to unlike post'})
        }
    
    finally:
        connection.close()
        
def dislike_post(user_id, post_id):
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    logger.info("Disliking Post")
    try:
        with connection.cursor() as cursor:
            # Check if post is already disliked
            check_dislike_sql = "SELECT COUNT(*) FROM dislikes WHERE user_id = %s AND post_id = %s"
            cursor.execute(check_dislike_sql, (user_id, post_id))
            already_disliked = cursor.fetchone()[0] > 0
            
            # Check if post is liked
            check_like_sql = "SELECT COUNT(*) FROM likes WHERE user_id = %s AND post_id = %s"
            cursor.execute(check_like_sql, (user_id, post_id))
            already_liked = cursor.fetchone()[0] > 0

            if already_disliked:
                # If already disliked, remove the dislike
                delete_dislike(user_id, post_id)
            else:
                # Add dislike
                insert_dislike_sql = "INSERT INTO dislikes (user_id, post_id) VALUES (%s, %s)"
                cursor.execute(insert_dislike_sql, (user_id, post_id))
                increment_dislike_sql = "UPDATE posts SET dislike_count = COALESCE(dislike_count, 0) + 1 WHERE id = %s"
                cursor.execute(increment_dislike_sql, (post_id,))

                if already_liked:
                    # If liked, remove the like
                    delete_like_sql = "DELETE FROM likes WHERE user_id = %s AND post_id = %s"
                    cursor.execute(delete_like_sql, (user_id, post_id))
                    decrement_like_sql = "UPDATE posts SET like_count = COALESCE(like_count, 0) - 1 WHERE id = %s"
                    cursor.execute(decrement_like_sql, (post_id,))
            
            connection.commit()

        return {
            'statusCode': 201,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'message': 'Post dislike status updated successfully'})
        }

    except Exception as e:
        connection.rollback()
        logger.error(f"Error disliking post: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'error': 'Failed to update dislike status'})
        }

    finally:
        connection.close()
        
def delete_dislike(user_id, post_id):
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    
    try:
        logger.info("Deleting dislike")
        
        with connection.cursor() as cursor:
            # Delete from dislikes table
            delete_dislike_sql = """
            DELETE FROM dislikes
            WHERE user_id = %s AND post_id = %s
            """
            cursor.execute(delete_dislike_sql, (user_id, post_id))
            
            # Decrement dislike count on post
            decrement_dislike_sql = """
            UPDATE posts
            SET dislike_count = dislike_count - 1
            WHERE id = %s
            """
            cursor.execute(decrement_dislike_sql, (post_id,))
            
            connection.commit()
        
        return {
            'statusCode': 200,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'message': 'Dislike deleted successfully'})
        }
    
    except Exception as e:
        connection.rollback()
        logger.error(f"Error deleting dislike: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'error': 'Failed to delete dislike'})
        }
    
    finally:
        connection.close()

def get_post_by_post_id(event):
    try:
        # Get the post_id from the path parameters
        logger.info(event)
        id = event['queryStringParameters']['id']
        logger.info("Getting post for post id: %s", id)
        logger.info("creating db connection")
        connection = pymysql.connect(host=DB_HOST,
                                     user=DB_USER,
                                     password=DB_PASSWORD,
                                     database=DB_NAME)
        try:
            with connection.cursor() as cursor:
                sql = "SELECT id, user_id, content, like_count, dislike_count FROM posts WHERE id = %s"
                cursor.execute(sql, (id,))
                logger.info("Fetching post")
                post = cursor.fetchone()

            connection.commit()

            if post:
                return {
                    'statusCode': 200,
                    'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                    'body': json.dumps({'post': post})
                }
            else:
                return {
                    'statusCode': 404,
                    'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                    'body': json.dumps({'error': 'Post not found'})
                }
        except Exception as e:
            logger.info(str(e))
            connection.rollback()
            raise e
        finally:
            connection.close()
    except Exception as e:
        logger.info(str(e))
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'error': str(e)})
        }

def get_all_posts_by_user_id(event):
    try:
        # Get the user_id from the path parameters
        user_id = event['queryStringParameters']['user_id']
        logger.info("Getting all posts for user: %s", user_id)
        connection = pymysql.connect(host=DB_HOST,
                                     user=DB_USER,
                                     password=DB_PASSWORD,
                                     database=DB_NAME)
        try:
            with connection.cursor() as cursor:
                sql = "SELECT id, user_id, username, content, like_count, dislike_count FROM posts WHERE user_id = %s"
                cursor.execute(sql, (user_id,))
                posts = cursor.fetchall()

            connection.commit()

            return {
                'statusCode': 200,
                'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                'body': json.dumps({'posts': posts})
            }
        except Exception as e:
            logger.error(str(e))
            connection.rollback()
            raise e
        finally:
            connection.close()
            
    except Exception as e:
        logger.error(str(e))
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'error': str(e)})
        }

def handle_post(event):
    try:
        # Parse the request body
        request_body = json.loads(event['body'])
        user_id = request_body.get('user_id')
        post_text = request_body.get('post_text')
        media_filename = request_body.get('media_filename')
        content_type = request_body.get('content_type')
        username = request_body.get('username')

        # Validate input
        if not user_id or not post_text or not username:
            return {
                'statusCode': 400,
                'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                'body': json.dumps({'error': 'Missing required parameters'})
            }

        # Save post data to the database
        post_id = save_post_to_database(user_id, username, post_text)
        
        # Generate a pre-signed URL for the media upload if media exists
        presigned_url = None
        media_key = None

        if media_filename:
            media_key = f'uploads/{user_id}/{media_filename}'
            presigned_url = generate_presigned_url(MEDIA_BUCKET_NAME, media_key, content_type)
            
            if not presigned_url:
                raise Exception("Failed to generate pre-signed URL")

        return {
            'statusCode': 200,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'post_id': post_id, 'upload_url': presigned_url, 'media_key': media_key})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
            'body': json.dumps({'error': str(e)})
        }

def generate_presigned_url(bucket_name, object_name, content_type, expiration=3600):
    try:
        response = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket_name, 'Key': object_name, 'ContentType':content_type},
            ExpiresIn=expiration
        )
        return response
    except Exception as e:
        print(f"Error generating pre-signed URL: {e}")
        return None

def save_post_to_database(user_id, username, post_text):
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=DB_NAME)
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO posts (user_id, username,  content) VALUES (%s, %s, %s)"
            cursor.execute(sql, (user_id, username, post_text))
            post_id = cursor.lastrowid
        connection.commit()
        return post_id
    except Exception as e:
        connection.rollback()
        print(str(e))
        raise e
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


