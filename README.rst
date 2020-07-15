DYNAMORE
--------

Dynamore is extremely simple Python library for **managing entities** on DynamoDb. It's designed to be used together with REST API and only supports **single table design**.

Installation
------------

Install from Pypi:

.. code-block:: bash

   $ pip install dynamore

Quick start
-----------

Dynamore doesn't manage your tables so create DynamoDb table beforehand e.g. provisioning by CloudFormation.

Here's the simple example on how to create Person schema and store it to DynamoDb:

.. code-block:: Python

    from Dynamore.dynamodb_proxy import DynamoDbProxy
    from Dynamore.entity import Entity


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
    data = {
        "name": "Jeanne",
        "age": 123,
        "gender": "female", 
        "id_number": "123456"
    }
    _ = db.post(entity_class=Person, data=data)
    # Get single item
    item = db.get(
        entity_class=Person, data={"id_number": data["id_number"]}
    )
    # Get all items of type "Person"
    items = dynamodb_proxy.get(entity_class=Person)

First a new entity class Person defined. It's **schema** is defined using jsonschema and **id attribute** defines the name of the attribute that is used for uniqueness.

Dynamore stores data to DynamoDb in the following format:

+--------+--------+--------+-----+--------+
| PK     | SK     | name   | age | gender |
+========+========+========+=====+========+
| PERSON | 123456 | Jeanne | 123 | female | 
+--------+--------+--------+-----+--------+

By default entity uses partition key "PK" and sort key "SK" value but you can define them otherwise by overriding pr_keys-method.