import os
import json
import boto3
import urllib.parse

iot_client = boto3.client('iot-data', region_name='us-east-1')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    # Monta a mensagem
    message = {
        'bucket': bucket_name,
        'object_key': object_key,
        'message': 'Novo arquivo chegou no S3!'
    }

    response = iot_client.publish(
        topic=os.environ["iot-topic"],
        qos=1,
        payload=json.dumps(message)
    )

    return {
        'statusCode': 200,
        'body': 'Mensagem publicada com sucesso'
    }
