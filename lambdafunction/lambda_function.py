import json
import boto3
import pymysql
import logging
from collections import defaultdict
import os

# Initialize AWS clients
s3_client = boto3.client('s3')

# Constants
MEDIA_BUCKET_NAME = os.environ['MEDIA_BUCKET_NAME']
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
POST_DB = os.environ['POST_DB']
POSTS_DB_NAME = os.environ['POST_DB']
MEDIA_DB_NAME = os.environ['MEDIA_DB']
COMMENT_DB_NAME = os.environ['COMMENT_DB']


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
            logger.info("Post method: " + str(path))
            if path == '/post-management/like':
                user_id = query_parameters.get('user_id')
                post_id = query_parameters.get('post_id')
                return like_post(user_id, post_id)
            elif path == '/post-management/dislike':
                user_id = query_parameters.get('user_id')
                post_id = query_parameters.get('post_id')
                return dislike_post(user_id, post_id)
            elif path == '/post-management/post':
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
        database=POST_DB
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
        database=POST_DB
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
        database=POST_DB
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
        database=POST_DB
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
                                     database=POST_DB)
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
        query_parameters =  event['queryStringParameters']
        user_id = query_parameters['user_id']
        current_user_id = query_parameters['current_user_id']
        logger.info("Getting all posts for user: %s", user_id)
        connection = pymysql.connect(host=DB_HOST,
                                     user=DB_USER,
                                     password=DB_PASSWORD,
                                     database=POST_DB)
        try:
            with connection.cursor() as cursor:
                sql = "SELECT id, user_id, username, content, like_count, dislike_count FROM posts WHERE user_id = %s"
                cursor.execute(sql, (user_id,))
                tmp_posts = cursor.fetchall()
                posts = []

            connection.commit()
            
            for post in tmp_posts:
                post_dict = {
                    "id": post[0],
                    "user_id": post[1],
                    "username": post[2],
                    "content": post[3],
                    "like_count": post[4],
                    "dislike_count": post[5]
                }
                
                posts.append(post_dict)
            
            results = process_search_results(posts, user_id, current_user_id)
            
            return {
                'statusCode': 200,
                'headers': {
                      "Access-Control-Allow-Origin": "*", 
                      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                      "Access-Control-Allow-Credentials": 'true',
                    },
                'body': json.dumps({'posts': results})
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
        
def get_post_ids(posts):
    post_ids = []
    
    for post in posts:
        post_ids.append(post['id']) 
        
    return post_ids
    
def process_search_results(posts, user_id, current_user_id):
    logger.info("Setting Variables")
    result = []
    
    try:
        logger.info("Separating Post IDs")
        post_ids = get_post_ids(posts)
        
        if post_ids:
            logger.info("Getting Likes for posts")
            user_likes = get_user_likes(current_user_id)
            
            logger.info("Getting dislikes for posts")
            user_dislikes = get_user_dislikes(current_user_id)
            
            logger.info("Getting comments for the posts")
            comments = get_comments_by_post_id(post_ids)
            
            logger.info("Getting Media Metadata for the posts")
            media_metadata = get_media_metadata_by_post_ids(post_ids)
            
            result = combine_posts_with_media(posts, comments, media_metadata, user_likes, user_dislikes)
        
        return result

        
        
        return processed_results
    except Exception as e:
        logger.info(str(e))
        raise e
        
def get_media_metadata_by_post_ids(post_ids):
    if not post_ids:
        return []
    
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=MEDIA_DB_NAME)
    logger.info("Get metadata for media in post")
    post_id_tuple = tuple(post_ids)
    try:
        with connection.cursor() as cursor:
            sql = "select user_id, post_id, s3_key, url, size, type, expiresAt from media_metadata where post_id in %s"
            cursor.execute(sql, (post_id_tuple,))
            results = cursor.fetchall()
            logger.info("media metadata")
            logger.info(results)
            media_list = []
            for media in results:
                media_dict = {
                    "user_id": media[0],
                    "post_id": media[1],
                    "s3_key": media[2],
                    "url" : media[3],
                    "size": media[4],
                    "type": media[5],
                    "expiresAt": media[6]
                }
                media_list.append(media_dict)
            logger.info("media list")
            logger.info(media_list)
        return media_list
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        connection.close()

def get_comments_by_post_id(post_ids):
    if not post_ids:
        return []
    
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=COMMENT_DB_NAME)
    logger.info("Get comments for posts")
    post_id_tuple = tuple(post_ids)
    logger.info("post id's")
    logger.info(post_id_tuple)
    try:
        with connection.cursor() as cursor:
            sql = "select id, user_id, post_id, content, created_at, username from comments where post_id in %s"
            cursor.execute(sql, (post_id_tuple,))
            results = cursor.fetchall()
            logger.info("post comments")
            logger.info(results)
            comment_dict = defaultdict(list)
            
            for comment in results:
                
                comment_object = {
                    "id":comment[0],
                    "post_id":comment[2],
                    "user_id":comment[1],
                    "username":comment[5],
                    "content":comment[3],
                    "created_at":comment[4].strftime('%Y-%m-%d %H:%M:%S')
                }
                
                comment_dict[comment_object['post_id']].append(comment_object)
                
            logger.info("comment dictionary")
            logger.info(comment_dict)
        return comment_dict
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        connection.close()
        
def get_user_likes(user_id):
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=POSTS_DB_NAME)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT post_id FROM likes WHERE user_id = %s", (user_id,))
            likes = cursor.fetchall()
        return {like[0] for like in likes}
    except Exception as e:
        logger.error(f"Error fetching user likes: {e}")
        return set()
        
def get_user_dislikes(user_id):
    connection = pymysql.connect(host=DB_HOST,
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 database=POSTS_DB_NAME)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT post_id FROM dislikes WHERE user_id = %s", (user_id,))
            dislikes = cursor.fetchall()
        return {dislike[0] for dislike in dislikes}
    except Exception as e:
        logger.error(f"Error fetching user dislikes: {e}")
        return set()
        
def combine_posts_with_media(posts, comments, media_metadata, user_likes, user_dislikes):
    logger.info("Combining media to the post")
    logger.info(media_metadata)
    media_dict = {}
    for media in media_metadata:
        post_id = media['post_id']
        if post_id not in media_dict:
            media_dict[post_id] = []
        media_dict[post_id].append(media)
        
    logger.info(user_likes)
    
    for post in posts:
        logger.info(post)
        post['media_metadata'] = media_dict.get(post['id'], [])
        post['comments'] = comments.get(post['id'],[])
        post['likedByUser'] = post['id'] in user_likes
        post['dislikedByUser'] = post['id'] in user_dislikes
    
    return posts

def handle_post(event):
    try:
        logger.info("Handling Post")
        # Parse the request body
        request_body = json.loads(event['body'])
        user_id = request_body.get('user_id')
        post_text = request_body.get('post_text')
        media_files = request_body.get('media_files', [])  # Expecting a list of media files
        username = request_body.get('username')
        logger.info("Validating post content")

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

        # Generate pre-signed URLs for the media uploads if media exists
        upload_urls = []
        media_keys = []

        for media_file in media_files:
            media_filename = media_file.get('media_filename')
            content_type = media_file.get('content_type')

            if media_filename and content_type:
                media_key = f'uploads/{user_id}/{media_filename}'
                presigned_url = generate_presigned_url(MEDIA_BUCKET_NAME, media_key, content_type)

                if not presigned_url:
                    raise Exception("Failed to generate pre-signed URL")

                upload_urls.append(presigned_url)
                media_keys.append(media_key)

        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                "Access-Control-Allow-Credentials": 'true',
            },
            'body': json.dumps({'post_id': post_id, 'upload_urls': upload_urls, 'media_keys': media_keys})
        }

    except Exception as e:
        logger.error(f"Error handling post: {str(e)}")
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
        logger.info("Generating Upload url")
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
                                 database=POST_DB)
    try:
       
        with connection.cursor() as cursor:
            sql = "INSERT INTO posts (user_id, username,  content) VALUES (%s, %s, %s)"
            cursor.execute(sql, (user_id, username, post_text))
            logger.info("Saving post to db, Username: %s", username)
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


