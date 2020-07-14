import os
import pytest

from dynamoer.dynamodb_proxy import DynamoDbProxy
from dynamoer.entity import Entity

@pytest.fixture
def dynamodb_proxy(empty_entity_table):
    return DynamoDbProxy(table_name=os.getenv("DDB_ENTITYTABLE"))

def test_post_item(dynamodb_proxy):
    item = dynamodb_proxy.post_item(entity_class=Entity, data={"name": "Test"})
    assert item is not None and type(item) is dict

def test_get_item(dynamodb_proxy):
    item_orig = dynamodb_proxy.post_item(entity_class=Entity, data={"name": "Test"})
    item = dynamodb_proxy.get_item(entity_class=Entity, data=item_orig)
    assert item is not None and type(item) is dict
    assert item == item_orig

def test_put_item(dynamodb_proxy):
    # TODO:
    pass

def test_delete_item(dynamodb_proxy):
    item_orig = dynamodb_proxy.post_item(entity_class=Entity, data={"name": "Test"})
    item = dynamodb_proxy.delete_item(entity_class=Entity, data=item_orig)
    assert item is not None and type(item) is dict
    assert item == item_orig
