import json
import boto3
from botocore.exceptions import ClientError

# Initialize the S3 client
s3 = boto3.client('s3')
s3_r = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('tenkuuJankenHighScore')

def get_user_key(BUCKET_NAME, FOLDER_NAME):
    
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FOLDER_NAME)
        contents = response.get('Contents', [])
        only_result = contents[0]
        return only_result['Key']
    except IndexError:
        return 'No data'

def get_user_from_bucket(BUCKET_NAME, FOLDER_NAME):
    # Get existing JSON file from S3 Bucket
    prev_player_key = get_user_key(BUCKET_NAME, FOLDER_NAME)
    
    if prev_player_key == 'No data':
        return json.loads('{}')
    
    # Make Bucket option
    bucket = s3_r.Bucket(BUCKET_NAME)
    obj = bucket.Object(prev_player_key)
    
    
    # Get JSON file using key
    response = obj.get()
    body = response['Body'].read()
    json_data = json.loads(body.decode('utf-8'))
    print(body)
    return json_data

def delete_user_selection(BUCKET_NAME,s3_key):
    s3.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
    
def janken(a_user, a_select, b_user, b_select):
    valid_selections = ['rock', 'paper', 'scissors']
    
    if a_select not in valid_selections or b_select not in valid_selections:
        return "Invalid selection. Selections must be 'rock', 'paper', or 'scissors'."
    
    if a_select == b_select:
        return ""
    
    rules = {
        'rock': 'scissors',
        'scissors': 'paper',
        'paper': 'rock'
    }
    
    if rules[a_select] == b_select:
        return a_user
    else:
        return b_user

def update_user_high_score(user):
    try:
        # Check if the user exists
        #print(user)
        response = table.get_item(Key={'User': user})
        #print(response)
        if 'Item' in response:
            # User exists, update their high score by +1
            table.update_item(
                Key={'User': user},
                UpdateExpression='SET score = score + :increment',
                ExpressionAttributeValues={':increment': 1},
                ReturnValues='UPDATED_NEW'
            )
            update_msg = f"User '{user}' high score updated."
            #print(update_msg)
            return update_msg
        else:
            # User does not exist, add them with a high score of 1
            table.put_item(Item={'User': user, 'Score': 1})
            err_msg = f"User '{user}' added with a high score of 1."
            #print(err_msg)
            return err_msg
    
    except ClientError as e:
        return f"Error: {e.response['Error']['Message']}"

def lambda_handler(event, context):
    # Get input from event
    BUCKET_NAME = event['Bucket']
    FOLDER_NAME = event['Folder']
    challenger_user = event['User']
    challenger_selection = event['Selection']
    
    # Get previous user from S3 Bucket
    prev_player = get_user_from_bucket(BUCKET_NAME, FOLDER_NAME)
    # print(f"{prev_player['user']} + {prev_player['selection']} ")
    
    # If error / empty JSON, return error
    if len(prev_player) == 0:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'No user in bucket.'})
        }
    
    # Do janken, return the winning players name or blank if tie
    winner = janken(challenger_user, challenger_selection, prev_player['user'], prev_player['selection'])
    print(winner)
    
    # Remove JSON file from Bucket
    delete_user_selection(BUCKET_NAME, get_user_key(BUCKET_NAME, FOLDER_NAME))
    
    # Add +1 to high score
    if winner == '':
        return {
            'statusCode': 200,
            'body': 'Tie'
        }
    else:
        db_message = update_user_high_score(winner)
        print(db_message)
    
    # Return winner to tenkuuJankenEntry
    return {
        'statusCode': 200,
        'body' : json.dumps({'message': f'The winner is {winner}'}),
        'winner' : winner
    }
    
 