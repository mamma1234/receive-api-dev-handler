import json

def success(body):
    return {
        "statusCode": 200,
        "body": json.dumps(body)
    }

def error(status, message):
    return {
        "statusCode": status,
        "body": json.dumps({"error": message})
    }