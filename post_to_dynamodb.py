import json
import os
import time
import uuid
import traceback
import boto3
from decimal import Decimal

dynamo = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])

def lambda_handler(event, context):
    try:
        body = event.get('body')
        if body:
            msg = json.loads(body, parse_float=Decimal)
        else:
            msg = event if isinstance(event, dict) else json.loads(json.dumps(event), parse_float=Decimal)
            
        item = {
            "id": uuid.uuid4().hex,
            "deviceId": str(msg.get("deviceId", "unknown")),
            "sensorModel":str(msg.get("sensorModel", "unknown")),
            "measureUnit":str(msg.get("measureUnit", "unknown")),
            "location":str(msg.get("location", "unknown")),
            "dataType":str(msg.get("dataType", "unknown")),
            "data":float(msg.get("data", 0.0)),
            "ts": int(time.time()*1000),
        }
        item = json.loads(json.dumps(item), parse_float=Decimal)
        dynamo.put_item(Item=item)
        
        response = {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",  # Para suporte CORS
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Allow-Methods": "POST,OPTIONS"
            },
            "body": json.dumps({"status": "ok", "saved": {"deviceId":item["deviceId"], "ts": item["ts"]}})
        }
        return response
    except Exception as e:
        error_response = {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Allow-Methods": "POST,OPTIONS"
            },
            "body": json.dumps({
                "status": "error",
                "detail": str(e),
                "traceback": traceback.format_exc()
            })
        }
        return error_response