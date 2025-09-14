import json
import os
import time
import uuid
import traceback
import boto3
from decimal import Decimal

dynamo = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])

def lambda_handler(event, context):
    # Ex.: {"deviceId":"sensor-001","temp":24.2,"hum":60}
    try:
        msg = event if isinstance(event, dict) else json.loads(json.dumps(item), parse_float=Decimal)
        item = {
            "deviceId": str(msg.get("deviceId", "unknown")),
            "ts": int(time.time()*1000),
            "id": uuid.uuid4().hex,
            "temp": float(msg.get("temp", 0)),
            "hum": float(msg.get("hum", 0)),
            "raw": json.dumps(msg)
        }
        item = json.loads(json.dumps(item), parse_float=Decimal)
        dynamo.put_item(Item=item)
        return {"status": "ok", "saved": {"deviceId":item["deviceId"], "ts": item["ts"]}}
    except Exception as e:
        return {
        "status": "error",
        "detail": str(e),
        "traceback": traceback.format_exc()
    }