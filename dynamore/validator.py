# from jsonschema.exceptions import ValidationError
from uuid import UUID

from jsonschema import Draft7Validator, validators


# Build a new type checker
def is_uuid4(checker, inst):
    try:
        UUID(inst)
        return True
    except ValueError:
        return False


uuid4_check = Draft7Validator.TYPE_CHECKER.redefine(u"uuid4", is_uuid4)
# Build a validator with the new type checker
Validator = validators.extend(Draft7Validator, type_checker=uuid4_check)


def validate(schema: dict, data: dict):
    """
    Validate date against the given schema.
    """
    try:
        Validator(schema=schema).validate(data)
    except Exception as e:
        raise ValueError(e)
