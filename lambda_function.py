import json
import urllib3
from utils.response import success, error
from services.token_service import issue_token_remote, verify_token_remote
from services.db import save_integration_payload, save_customization_payload

http = urllib3.PoolManager()

BASE_URL = "https://api.test-spot.com/api/v1"

def lambda_handler(event, context):
    try:
        raw_path = event.get("rawPath") or event.get("path", "")
        method = event.get("requestContext", {}).get("http", {}).get("method", "")

        # ===========================
        # Basic Test
        # ===========================
        if raw_path == "/api/v1":
            return success({
                "path": raw_path,
                "message": "API OK"
            })

        # ===========================
        # Basic Test1
        # ===========================
        if raw_path == "/api/v1/test":
            return success({
                "path": raw_path,
                "message": "API OK"
            })


        # -----------------------------
        # Token Issue
        # -----------------------------
        if raw_path == "/api/v1/oauth/access_token":
            params = event.get("queryStringParameters") or {}
            try:
                result = issue_token_remote(
                    params.get("client_id"),
                    params.get("client_secret"),
                    params.get("grant_type", "client_credentials")
                )
                return success(result)
            except Exception as e:
                return error(500, str(e))

        # -----------------------------
        # Token Verify
        # -----------------------------
        if raw_path == "/api/v1/oauth/verify":
            headers = event.get("headers") or {}
            auth_header = headers.get("Authorization") or headers.get("authorization")
            if not auth_header:
                return error(401, "Authorization header required")
            try:
                result = verify_token_remote(auth_header)
                return success(result)
            except Exception as e:
                return error(401, str(e))

        # -----------------------------
        # Order Receive → DB Save
        # -----------------------------
        if raw_path == "/api/v1/customer-order/integrations" and method == "POST":
            headers = event.get("headers") or {}
            auth_header = headers.get("Authorization") or headers.get("authorization")
            if not auth_header:
                return error(401, "Authorization header required")

            try:
                verify_result = verify_token_remote(auth_header)
            except Exception as e:
                return error(401, f"Token verify failed: {str(e)}")
                
            agent = verify_result.get("agent") or {}
            client_id = agent.get("id")

            if not client_id:
                return error(400, "agent.id (client_id) not found in token verify response")
                    
            body = json.loads(event.get("body") or "{}")

            try:
                parent_id = save_integration_payload(body, client_id=client_id)
            except Exception as e:
                return error(500, f"DB save failed: {str(e)}")
            
            try:
                status, confirm_result = call_confirm_api(parent_id, auth_header)
                if status not in (200, 201):
                    return error(
                        500,
                        f"Confirm API failed (status={status}): {confirm_result}"
                    )
            except Exception as e:
                return error(500, str(e))
            
            return success({
                "message": "saved + confirmed",
                "integration_id": parent_id,
                "client_id": client_id,
                "confirm_status": status
            })            

        if raw_path == "/api/v1/customer-order/customizations" and method == "POST":
            headers = event.get("headers") or {}
            auth_header = headers.get("Authorization") or headers.get("authorization")
            if not auth_header:
                return error(401, "Authorization header required")

            try:
                verify_result = verify_token_remote(auth_header)
            except Exception as e:
                return error(401, f"Token verify failed: {str(e)}")
                
            agent = verify_result.get("agent") or {}
            client_id = agent.get("id")

            if not client_id:
                return error(400, "agent.id (client_id) not found in token verify response")
                    
            body = json.loads(event.get("body") or "{}")

            try:
                parent_id = save_customization_payload(body, client_id=client_id)
            except Exception as e:
                return error(500, f"DB save failed: {str(e)}")
            
            try:
                status, confirm_result = call_confirm_api(parent_id, auth_header)
                if status not in (200, 201):
                    return error(
                        500,
                        f"Confirm API failed (status={status}): {confirm_result}"
                    )
            except Exception as e:
                return error(500, str(e))
            
            return success({
                "message": "saved + confirmed",
                "integration_id": parent_id,
                "client_id": client_id,
                "confirm_status": status
            })            

        return error(404, "Not Found")

    except Exception as e:
        print("Lambda Error:", str(e))
        return error(500, "Internal Server Error")
    

def call_confirm_api(integration_id: int, auth_header: str):
    url = f"{BASE_URL}/customer-order/integration/{integration_id}/confirms"

    try:
        res = http.request(
            "POST",
            url,
            headers={
                "Authorization": auth_header,
                "Content-Type": "application/json",
                "X-Use-External-Customer": "1"
            },
            body=json.dumps({})
        )
        return res.status, res.data.decode("utf-8")
    except Exception as e:
        raise Exception(f"Confirm API error: {str(e)}")