import os
import pytest

from dynamore.dynamodb_proxy import DynamoDbProxy
from dynamore.entity import Entity


class Person(Entity):
    SCHEMA = {
        "title": "PERSON",
        "type": "object",
        "required": ["name", "age", "gender", "id_number"],
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer", "min": 0, "max": 123},
            "gender": {"type": "string", "enum": ["male", "female"]},
            "id_number": {"type": "string"},
        },
        "additionalProperties": False,
    }

    ID_ATTRIBUTE = "id_number"


@pytest.fixture
def dynamodb_proxy(empty_entity_table):
    return DynamoDbProxy(table_name=os.getenv("DDB_ENTITYTABLE"))


@pytest.fixture
def person_data():
    return {"name": "Jeanne", "age": 123, "gender": "female", "id_number": "123456"}


def test_post(dynamodb_proxy, person_data):
    item = dynamodb_proxy.post(entity_class=Person, data=person_data)
    assert item is not None and type(item) is dict


def test_get(dynamodb_proxy, person_data):
    item_orig = dynamodb_proxy.post(entity_class=Person, data=person_data)
    item = dynamodb_proxy.get(
        entity_class=Person, data={"id_number": item_orig["id_number"]}
    )
    assert item is not None and type(item) is dict
    assert item == item_orig


def test_get_all(dynamodb_proxy, person_data):
    item_orig = dynamodb_proxy.post(entity_class=Person, data=person_data)
    response = dynamodb_proxy.get(entity_class=Person)
    assert response is not None and type(response) is list
    assert item_orig == response.pop()


def test_put(dynamodb_proxy, person_data):
    # TODO:
    pass


def test_delete(dynamodb_proxy, person_data):
    item_orig = dynamodb_proxy.post(entity_class=Person, data=person_data)
    item = dynamodb_proxy.delete(
        entity_class=Person, data={"id_number": item_orig["id_number"]}
    )
    assert item is not None and type(item) is dict
    assert item == item_orig


def test_readme_example(dynamodb_proxy):
    class Person(Entity):
        SCHEMA = {
            "title": "PERSON",
            "type": "object",
            "required": ["name", "age", "gender", "id_number"],
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "min": 0, "max": 123},
                "gender": {"type": "string", "enum": ["male", "female"]},
                "id_number": {"type": "string"},
            },
            "additionalProperties": False,
        }

        ID_ATTRIBUTE = "id_number"

    db = DynamoDbProxy(table_name="MyTable")
    data = {"name": "Jeanne", "age": 123, "gender": "female", "id_number": "123456"}
    _ = db.post(entity_class=Person, data=data)
    # Get single item
    item = db.get(entity_class=Person, data={"id_number": data["id_number"]})
    # Get all items of type "Person"
    items = dynamodb_proxy.get(entity_class=Person)
