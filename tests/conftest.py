import logging
import os
from uuid import uuid4

import boto3
import pytest

# Disable botocore annoying "Found credentials in environment variables."
logging.getLogger("botocore").setLevel(logging.CRITICAL)

AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
session = boto3.session.Session()

@pytest.fixture
def empty_entity_table(monkeypatch):
    # Copy .envTEMPLATE to .env to populate env variables for test
    assert os.getenv("AWS_ACCESS_KEY_ID")
    assert os.getenv("AWS_SECRET_ACCESS_KEY")
    assert os.getenv("AWS_DEFAULT_REGION")
    assert os.getenv("DDB_ENDPOINT")
    ddb_tablename = str(uuid4())
    monkeypatch.setenv("DDB_ENTITYTABLE", ddb_tablename)
    dynamodb = session.resource("dynamodb", endpoint_url=os.getenv("DDB_ENDPOINT"))
    _ = dynamodb.create_table(
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        TableName=os.getenv("DDB_ENTITYTABLE"),
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST"
    )
    table = dynamodb.Table(ddb_tablename)
    yield table
    # Once test has been completed table will be deleted
    _ = table.delete()