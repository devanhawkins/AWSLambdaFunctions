import json
import requests
import boto3
from datetime import datetime
#from botocore.vendored import requests

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('LocationWeather')

# Initialize S3 Bucket info
s3 = boto3.client('s3')
BUCKET_NAME = 'dai-corp-weather-bucket'

def lambda_handler(event, context):
    # Extract location from query parameters
    location = event['queryStringParameters']['location']
    
    # API credentials and endpoint (use your own API key)
    api_key = 'bd5e378503939ddaee76f12ad7a97608'
    weather_url = f'http://api.openweathermap.org/data/2.5/weather?q={location}&appid={api_key}&units=metric'

    # Fetch weather data
    response = requests.get(weather_url)
    weather_data = response.json()

    # Check for errors in the response
    if response.status_code != 200 or 'main' not in weather_data:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Could not retrieve weather data'})
        }
    
    # Extract required data
    weather_info = {
        'location': location,
        'temperature': round(weather_data['main']['temp']),
        'description': weather_data['weather'][0]['description'],
        'humidity': weather_data['main']['humidity'],
        'pressure': weather_data['main']['pressure'],
        'timestamp': datetime.now().isoformat()
    }
    
    # Save the data to DynamoDB
    #table.put_item(Item=weather_info)
    
    # Save the data to S3
    
    #request_id = context.aws_request_id
    #bucket = s3.Bucket(BUCKET_NAME)
    object_key_name = f"{location}/{datetime.now().isoformat()}.json"
    s3.put_object(Bucket=BUCKET_NAME, Key=object_key_name, Body=json.dumps(weather_info))
    #obj = bucket.Object(object_key_name)
    #r = obj.put(Body = json.dumps(json_data))
    
    return {
        'statusCode': 200,
        'body': json.dumps(weather_info)
    }
