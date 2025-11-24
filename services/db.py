import os
import json
import pymysql
import boto3
import urllib3
from botocore.exceptions import ClientError

# print("DEBUG_PYMYSQL_FILE:", pymysql.__file__)
# print("DEBUG_HAS_CONNECT:", hasattr(pymysql, "connect"))

# Secrets Manager 설정
SECRET_NAME = "arn:aws:secretsmanager:ap-northeast-2:041962616356:secret:test/Tms/Mysql-22SyEC"
REGION_NAME = "ap-northeast-2"

# 전역 캐시
_db_config = None

def test_internet():
    http = urllib3.PoolManager()
    try:
        r = http.request("GET", "https://aws.amazon.com")
        print("INTERNET OK:", r.status)
    except Exception as e:
        print("INTERNET ERROR:", str(e))

def load_db_config():
    """
    Secrets Manager에서 DB 접속 정보를 1회 로드하여 캐싱.
    환경변수(DB_HOST 등)가 설정돼 있으면 그걸 우선 사용하고,
    없으면 Secret에서 가져오도록 할 수도 있음.
    """
    global _db_config

    # 이미 로딩되어 있으면 그대로 반환
    if _db_config is not None:
        return _db_config

    secret_str = ""

    try:
        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager",
            region_name=REGION_NAME,
        )

        resp = client.get_secret_value(SecretId=SECRET_NAME)

        # SecretString -> JSON 파싱
        secret_str = resp.get("SecretString")
    except ClientError as e:
        print("Failed to load DB secret:", e)
        raise

    # if not secret_str:
    #     raise RuntimeError("SecretString is empty")

    try:
        secret = json.loads(secret_str)
    except Exception as e:
        print("SECRET PARSE ERROR:", str(e))
        raise

    _db_config = {
        "host": secret["hostPrivate"],
        "user": secret["username"],
        "password": secret["password"],
        "database": secret["dbname"],
    }

    return _db_config


def get_connection():
    cfg = load_db_config()
    print("DB CONFIG:", cfg)
    try:
        return pymysql.connect(
            host=cfg["host"],
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
    except Exception as e:
        print("DB CONNECT ERROR:", str(e))
        print("DB HOST:", cfg["host"])
        print("DB USER:", cfg["user"])
        raise


# -------------------------------
# 1) 부모 테이블 INSERT
# -------------------------------
def insert_customer_order_integration(client_id: int, definition_id: int = 0, header_str: str = "[]"):
    sql = """
        INSERT INTO customer_order_integrations
        (client_id, customer_order_integration_definition_id, header, created_at, updated_at)
        VALUES (%s, %s, %s, NOW(), NOW());
    """

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (client_id, definition_id, header_str,))
            conn.commit()
            return cursor.lastrowid
    finally:
        conn.close()

# -------------------------------
# 2) 자식 - Bulk Insert
# -------------------------------
def insert_customer_order_details(parent_id, orders, cells=None):
    if not orders:
        return

    attribute0 = orders[0]
    attribute_keys = list(attribute0.keys())

    if not cells:
        column_list = ", ".join(attribute_keys)
    else:
        column_names = []
        for cell in cells:
            header = cell.get("header")
            if header in attribute_keys:
                column_names.append(cell.get("field"))
        column_list = ", ".join(column_names)

    sql = f"""
        INSERT INTO customer_order_integration_details
        (customer_order_integration_id, {column_list}, status, created_at, updated_at)
        VALUES
        {", ".join(["(%s, " + ", ".join(["%s"] * len(attribute_keys)) + ", 0, NOW(), NOW())"] * len(orders))};
    """

    values = []
    for order in orders:
        values.append(parent_id)
        for key in attribute_keys:
            values.append(order.get(key))

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, values)
        conn.commit()
    finally:
        conn.close()


# -------------------------------
# 0) header 파싱
# -------------------------------
def parse_definitions(definition_json_str: str):
    """
    PHP의 getDefinitions() 함수와 동일한 결과를 반환.
    definition_json_str: DB에 저장돼 있는 definition JSON 문자열
    """

    # 1) JSON 문자열 → dict 변환
    defs = json.loads(definition_json_str)

    headers = []
    cells = []

    # 2) defs = { "attribute1": {header:..., column:..., type:...}, ... }
    for field, definition in defs.items():
        # header
        headers.append(definition.get("header"))

        # cells
        cells.append({
            "field": field,
            "column": definition.get("column"),
            "header": definition.get("header"),
            "type": definition.get("type")
        })

    header_str = json.dumps(headers, ensure_ascii=False)

    # 3) 반환
    return {
        "header_str": header_str,
        "cells": cells
    }


def get_integration_definition(client_id: int):
    definition = get_integration_definition_id(client_id)
    if not definition:
        return None
    
    def_json = definition["definition"]
    defs = parse_definitions(def_json)
    return {"id": definition["id"], "defs": defs}

# -------------------------------
# 0) 매핑 테이블 조회
# -------------------------------
def get_integration_definition_id(client_id: int):
    sql = """
        SELECT id, definition, mapping
        FROM customer_order_integration_definitions
        WHERE client_id = %s
        LIMIT 1;
    """

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (client_id,))
            row = cursor.fetchone()
            if row:
                # 그대로 필요한 모든 필드를 반환
                return {
                    "id": row["id"],
                    "definition": row.get("definition"),
                    "mapping": row.get("mapping")
                }
            return None
    finally:
        conn.close()

# -------------------------------
# 전체 저장 트랜잭션
# -------------------------------
def save_integration_payload(payload: dict, client_id: int):
    orders = payload.get("customer-orders", [])
    if not orders:
        print("NO ORDERS RECEIVED")
        return None

    definition = get_integration_definition(client_id)

    parent_id = insert_customer_order_integration(client_id=client_id, definition_id=definition["id"], header_str=definition["defs"]["header_str"])
    insert_customer_order_details(parent_id, orders)

    return parent_id

# -------------------------------
# 전체 저장 트랜잭션
# -------------------------------
def save_customization_payload(payload: dict, client_id: int):
    orders = payload.get("customer-orders", [])
    if not orders:
        print("NO CUSTOMIZATIONS RECEIVED")
        return None

    definition = get_integration_definition(client_id)
    # print(definition["id"])
    # print(definition["defs"]["header_str"])
    # print(definition["defs"]["cells"])
    parent_id = insert_customer_order_integration(client_id=client_id, definition_id=definition["id"], header_str=definition["defs"]["header_str"])
    insert_customer_order_details(parent_id, orders, definition["defs"]["cells"])

    return parent_id