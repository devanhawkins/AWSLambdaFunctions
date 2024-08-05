import json
import boto3
from botocore.exceptions import ClientError

# Define clients to interact with Lambda and S3
lamb = boto3.client('lambda')
s3 = boto3.client('s3')
sqs = boto3.client('sqs')
queue_url = 'https://sqs.ap-northeast-1.amazonaws.com/211125632834/AnnounceWinner'

# Define constants
BUCKET_NAME = 'dai-corp-janken-bucket'
FOLDER_NAME = 'players'

# check if user already exists using GET
def check_user(user):
    s3_key = f"{FOLDER_NAME}/{user}.json"
    
    try:
        check_response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=s3_key)
        # body = check_response['Body'].read()
        # print(body)
        return True
    except:
        print('User doesnt exist.')
        return False

# check if bucket is empty 
def is_bucket_empty():
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(BUCKET_NAME)
    count = bucket.objects.filter(Prefix=FOLDER_NAME)
    print('Bucket count: ' + str(len(list(count))))
    if len(list(count)) > 0:
        return False
    else:
        return True

# POST selection into bucket
def save_user_selection(user, selection):
    # Create a unique key for the S3 object
    s3_key = f"{FOLDER_NAME}/{user}.json"
    
    # Save the data to S3
    s3.put_object(
        Bucket=BUCKET_NAME, 
        Key=s3_key, 
        Body=json.dumps({'user': user, 'selection': selection})
    )
    
def send_message_to_sqs(message):
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message
        )
        print(f"Winner is: {response.get('MessageId')}")
    except ClientError as e:
        print(f"Failed to send message to SQS: {e.response['Error']['Message']}")
    

def lambda_handler(event, context):
    user = event['body']['user']
    selection = event['body']['selection']
    
    # Check if input JSON contains the required data
    if not user or not selection:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'User and selection are required'})
        }
        
    # Check if user already submitted a selection
    if check_user(user) == True:
        # 
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'User already exists'})
        }
    else:  
        # If bucket is empty, add a selection to the bucket
        #print(is_bucket_empty())
        if is_bucket_empty() == True:
            save_user_selection(user, selection)
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'User selection saved successfully'})
            }
        else:
            # If bucket is not empty and does not contain current user, shoubu (initiate janken)
            
            # Define the input paramaters to be passed to tenkuuJanken
            input_params = {
                "Bucket": BUCKET_NAME,
                "Folder": FOLDER_NAME,
                "User": user,
                "Selection": selection
            }
            
            # Call tenkuuJanken
            response = lamb.invoke(
                FunctionName = 'arn:aws:lambda:ap-northeast-1:211125632834:function:tenkuuJanken',
                InvocationType = 'RequestResponse',
                Payload = json.dumps(input_params)
                )
                
            # Get response from tenkuuJanken
            responseFromChild = json.load(response['Payload'])
            
            # Send result to SQS
            send_message_to_sqs(responseFromChild['winner'])
            
            return {
                'statusCode': 200,
                'body': responseFromChild
            }