from uuid import uuid4

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


def test_make_entity():
    entity = Entity({})


def test_make_person():
    entity = Person(
        {"name": "Jeanne", "age": 123, "gender": "female", "id_number": "123456"}
    )
