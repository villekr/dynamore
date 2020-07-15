from decimal import Decimal

import dynamore.validator as validator
from boto3.dynamodb.conditions import Key
from jsonschema.exceptions import ValidationError
from dynamore.utils import log


class Entity(object):

    SCHEMA = {
        "title": "ENTITY",
        "type": "object",
        "required": [],
        "properties": {},
        "additionalProperties": False,
    }

    ID_ATTRIBUTE = "uid"

    def __init__(self, data: dict):
        """
        Create a new object.
        Raises ValueError if data is not valid according to schema.
        """
        if self.SCHEMA is None or type(self.SCHEMA) is not dict:
            raise ValueError("SCHEMA not defined or type not dict.")
        if "title" not in self.SCHEMA:
            raise ValueError('"title" missing from SCHEMA.')
        try:
            validator.validate(schema=self.SCHEMA, data=data)
        except ValidationError as e:
            log.debug(e)
            raise ValueError
        except Exception as e:  # Some other error from validator
            log.debug(e)
            raise ValueError
        self.attributes = data

    # Override in subclass if needed.

    @classmethod
    def pr_keys(cls) -> list:
        """ Return attribute names used as database primary keys
        (partition and sort keys).
        """
        return ["PK", "SK"]

    @classmethod
    def make_identity(cls, data: dict) -> dict:
        """ Create identity from data.

        Throws ValueError if identity can't be made.
        """
        if data is None or cls.ID_ATTRIBUTE not in data:
            raise ValueError
        pr_keys = cls.pr_keys()
        identity = {
            pr_keys[0]: cls.SCHEMA["title"],
            pr_keys[1]: f"#{cls.SCHEMA['title']}#{data[cls.ID_ATTRIBUTE]}",
        }
        return identity

    @classmethod
    def query(cls, data: dict, table) -> dict:
        """ Query dynamodb either by entity type or get specific item
        """
        # Case 1: Try exact match
        # throws exception if uid not UUID v4
        pr_keys = cls.pr_keys()
        pk = pr_keys[0]
        try:
            log.debug("Try exact match")
            identity = cls.make_identity(data=data)
            log.debug(identity)
            return table.get_item(Key=identity)
        except ValueError:
            log.debug("Try exact match")
            # Don't do query if data contains "uid"
            # i.e. intention was to get specific item
            if data is not None and "uid" in data:
                log.debug("Exact match not found.")
                raise Exception("Not Found")
            log.debug("Get all similar entity types")
            exp = Key(pk).eq(cls.SCHEMA["title"])
            return table.query(KeyConditionExpression=exp)
        except Exception as e:
            log.error(e)
            raise Exception("Failed to get or query item.")

    def add_dynamic_attributes(self, item: dict) -> dict:
        """ Add entity specific dynamic attributes to item.
        data_with_identity-method calls this for additional attributes.
        """
        pass

    @classmethod
    def pre(cls, http_method: str, data: dict, existing_item: dict = None) -> bool:
        """ Perform any precondition checks or tasks
        http_method - name of http method
        data - input data
        existing_item - existing item's data in DynamoDb (if any)
        Return True if ok to continue
        Return False if precondition prevents continuance
        """
        return True

    @classmethod
    def post(cls, http_method: str, data: dict, existing_item: dict = None) -> bool:
        """ Perform any postcondition checks or tasks
        Return True if postconditions/tasks were successfull
        Return False if postconditions/tasks were successfull
        """
        return True

    # Instance methods implemented in Entity (no need to override)

    def identity(self) -> dict:
        """ Accessor for this object DynamoDb keys.
        """
        return self.make_identity(data=self.attributes)

    def data(self) -> dict:
        """ Accessor for this object data.
        """
        item = self.attributes.copy()  # Return copy of data
        # self.add_dynamic_attributes(item=item)
        return item

    def data_with_identity(self) -> dict:
        """ Accessor for this object data with DynamoDb keys.
        Adds pk and sk attributes to data.
        """
        pr_keys = self.pr_keys()
        identity = self.identity()
        item = self.attributes.copy()  # Return copy of data
        # DynamoDb feature:
        # 'Float types are not supported. Use Decimal types instead.'
        for key in item:
            if type(item[key]) is float:
                float_val = str(item[key])
                item[key] = Decimal(float_val)
        item[pr_keys[0]] = identity[pr_keys[0]]
        item[pr_keys[1]] = identity[pr_keys[1]]
        self.add_dynamic_attributes(item=item)
        return item

    # Class methods implemented in Entity (no need to override)

    @classmethod
    def decimal_to_float(cls, item):
        """ DynamoDb feature:
        'Float types are not supported. Use Decimal types instead.'
        -> Convert Decimals to float
        """
        for key in item:
            if type(item[key]) is dict:
                cls.decimal_to_float(item[key])
            elif type(item[key]) is Decimal:
                decimal_val = item[key]
                item[key] = float(decimal_val)

    @classmethod
    def filter_ddbkeys(cls, item: dict) -> dict:
        """Remove dynamodb primary keys from data. Modifies data.
        """
        # Check entity validity once read from ddb
        assert cls.pr_keys()[0] in item, "Call filter only for items read from db."
        assert cls.pr_keys()[1] in item, "Call filter only for items read from db."
        pr_keys = cls.pr_keys()
        # Pop pk and sk attributes away from data
        # If these operations fail there's something broken on the data model
        # database or query is incorrect.
        item.pop(pr_keys[0])
        item.pop(pr_keys[1])
        # Note! don't validate entity at this point
        # This allows to evolve DynamoDb data model over time as
        # extra attributes won't be harmfull.
        # Validate only when writing to dynamodb

        cls.decimal_to_float(item)
        return item

    @classmethod
    def filter_ddbkeys_list(cls, items: list) -> list:
        for item in items:
            cls.filter_ddbkeys(item)
        return items
