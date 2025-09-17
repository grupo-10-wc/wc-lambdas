import json
import os
import time
import datetime
import uuid
import traceback
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

dynamo = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])

def lambda_handler(event, context):
    if event.get('httpMethod') == 'GET':
        return handle_get_request(event, context)
    else:
        return handle_post_request(event, context)

def handle_get_request(event, context):
    try:
        query_params = event.get('queryStringParameters', {}) or {}
        
        start_ts = query_params.get('start_ts')
        end_ts = query_params.get('end_ts')
        device_id = query_params.get('deviceId')

        filter_expression = None
        
        if start_ts and end_ts:
            start_ts = int(start_ts)
            end_ts = int(end_ts)
            filter_expression = Key('ts').between(start_ts, end_ts)
        
        if device_id:
            device_filter = Key('deviceId').eq(device_id)
            filter_expression = device_filter & filter_expression if filter_expression else device_filter
        
        if filter_expression:
            result = dynamo.scan(FilterExpression=filter_expression)
        else:
            result = dynamo.scan(Limit=50)
            
        items = result.get('Items', [])
        
        for item in items:
            for key, value in item.items():
                if key == 'ts':
                    item[key] = datetime.datetime.fromtimestamp(int(value))
                elif isinstance(value, Decimal):

                        item[key] = float(value)
        
        response = {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Allow-Methods": "GET,OPTIONS"
            },
            "body": json.dumps({
                "status": "ok", 
                "count": len(items),
                "items": items
            }, default=lambda obj: float(obj) if isinstance(obj, Decimal) else str(obj))
        }
        return response
    except Exception as e:
        return create_error_response(e)

def handle_post_request(event, context):
    try:
        body = event.get('body')
        if body:
            payload = json.loads(body, parse_float=Decimal)
        else:
            payload = event if isinstance(event, dict) else json.loads(json.dumps(event), parse_float=Decimal)
        
        items_to_process = payload if isinstance(payload, list) else [payload]
        
        saved_items = []
        current_ts = int(time.time()*1000)
        
        for msg in items_to_process:
            item = {
                "id": uuid.uuid4().hex,
                "deviceId": str(msg.get("deviceId", "unknown")),
                "sensorModel": str(msg.get("sensorModel", "unknown")),
                "measureUnit": str(msg.get("measureUnit", "unknown")),
                "location": str(msg.get("location", "unknown")),
                "dataType": str(msg.get("dataType", "unknown")),
                "data": float(msg.get("data", 0.0)),
                "ts": msg.get("ts", current_ts),
            }
            item = json.loads(json.dumps(item), parse_float=Decimal)
            dynamo.put_item(Item=item)
            
            saved_items.append({
                "id": item["id"],
                "deviceId": item["deviceId"], 
                "ts": item["ts"]
            })
        
        response = {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Allow-Methods": "POST,OPTIONS"
            },
            "body": json.dumps({
                "status": "ok", 
                "count": len(saved_items),
                "saved": saved_items
            })
        }
        return response
    except Exception as e:
        return create_error_response(e)

def create_error_response(e):
    error_response = {
        "statusCode": 500,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
        },
        "body": json.dumps({
            "status": "error",
            "detail": str(e),
            "traceback": traceback.format_exc()
        }, default=lambda obj: float(obj) if isinstance(obj, Decimal) else str(obj))
    }
    return error_response