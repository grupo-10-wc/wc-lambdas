import os
import json
import boto3
import urllib.parse
import requests

s3_client = boto3.client('s3', region_name='us-east-1')
api_url = os.environ['API_GATEWAY_URL']

def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        
        print(f"New file added to S3: {object_key} in bucket: {bucket_name}")
        
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_key
        )
        
        file_content = response['Body'].read().decode('utf-8')
        json_data = json.loads(file_content)
        
        if not api_url:
            raise ValueError("API_GATEWAY_URL environment variable not set")
            
        headers = {
            'Content-Type': 'application/json'
        }
        
        api_response = requests.post(
            api_url, 
            data=json.dumps(json_data),
            headers=headers
        )
        
        print(f"API Gateway Response: Status Code: {api_response.status_code}")
        
        if api_response.status_code >= 200 and api_response.status_code < 300:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Successfully processed S3 JSON file and posted to API Gateway',
                    'file': object_key,
                    'api_response_status': api_response.status_code
                })
            }
        else:
            print(f"Error from API Gateway: {api_response.text}")
            raise Exception(f"API Gateway returned status code: {api_response.status_code}")
            
    except Exception as e:
        print(f"Error processing S3 event: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error processing S3 event: {str(e)}',
                'file': object_key if 'object_key' in locals() else 'unknown'
            })
        }
