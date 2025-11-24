import json
import urllib3

http = urllib3.PoolManager()

BASE_URL = "https://api.test-spot.com/api/v1/oauth"


# -------------------------------
# Token Issue
# -------------------------------
def issue_token_remote(client_id, client_secret, grant_type="client_credentials"):
    url = (
        f"{BASE_URL}/access_token"
        f"?client_id={client_id}&client_secret={client_secret}&grant_type={grant_type}"
    )

    res = http.request("POST", url)
    if res.status != 200:
        raise Exception(f"Token Issue Failed: {res.data.decode()}")

    return json.loads(res.data.decode())


# -------------------------------
# Token Verify
# -------------------------------
def verify_token_remote(authorization_header):
    url = f"{BASE_URL}/verify"

    res = http.request(
        "POST",
        url,
        headers={
            "Authorization": authorization_header,
            "Content-Type": "application/json",
        },
    )

    if res.status != 200:
        raise Exception(f"Token Verify Failed: {res.data.decode()}")

    return json.loads(res.data.decode())