import os

import boto3
from dynamore.entity import Entity
from dynamore.utils import log


class AlreadyExists(Exception):
    """Raised if trying to create an item and it already exists."""

    pass


class NotFound(Exception):
    """Trying to update non-existing item."""

    pass


class PreconditionFailed(Exception):
    """Precondition related to operation failed."""

    pass


class DynamoDbProxy(object):
    """ DynamoDbProxy implements api to manage entities on DynamoDb

    It offers http-like operations to get and add entity items.

    """

    def __init__(self, *, table_name: str):
        session = boto3.session.Session()
        self.dynamodb = session.resource(
            "dynamodb", endpoint_url=os.getenv("DDB_ENDPOINT")
        )
        self.table_name = table_name
        self.table = self.dynamodb.Table(table_name)

    def get(self, *, entity_class: Entity, data: dict = {}) -> list:
        response = entity_class.query(data=data, table=self.table)
        if "Item" in response:
            return entity_class.filter_ddbkeys(response["Item"])
        elif "Items" in response:
            return entity_class.filter_ddbkeys_list(response["Items"])
        else:
            return None

    def post(self, *, entity_class: Entity, data: dict = {}) -> dict:
        instance = entity_class(
            data=data
        )  # raise ValueError if data is not valid for given entity
        identity = instance.identity()
        log.debug(identity)
        response = self.table.get_item(Key=identity)
        log.debug(response)
        if "Item" in response:
            log.debug("Already exists.")
            raise AlreadyExists()
        try:
            item = instance.data_with_identity()
            log.debug(item)
            pre_status = entity_class.pre(http_method="POST", data=item)
            if pre_status is False:
                raise PreconditionFailed()
            response = self.table.put_item(Item=item)
            log.debug(response)
            post_status = instance.post(http_method="POST", data=item)
            log.debug(f"post-condition status: {post_status}")
            return instance.data()
        except Exception as e:
            log.error(f"post_item failed {e}")
            raise Exception(e)

    def put(self, *, entity_class: Entity, data: dict = {}) -> int:
        instance = entity_class(
            data=data
        )  # raise ValueError if data is not valid for given entity
        identity = instance.identity()
        log.debug(identity)
        response = self.table.get_item(Key=identity)
        log.debug(response)
        if "Item" not in response:
            log.debug("Not found")
            raise NotFound()
        item = instance.data_with_identity()
        log.debug(item)

        pre_status = entity_class.pre(
            http_method="PUT", data=item, existing_item=response["Item"]
        )
        if pre_status is False:
            raise PreconditionFailed()

        response = self.table.put_item(Item=item)
        log.debug(response)
        return instance.data()

    def patch(self, *, entity_class: Entity, data: dict = {}) -> int:
        identity = entity_class.make_identity(data=data)
        log.debug(identity)
        response = self.table.get_item(Key=identity)
        log.debug(response)
        if "Item" not in response:
            log.debug("Not found")
            raise NotFound()
        item = response["Item"]
        pre_status = entity_class.pre(
            http_method="PATCH", data=data, existing_item=item
        )
        if pre_status is False:
            raise PreconditionFailed()

        # Overwrite existing items' keys
        for key in data:
            item[key] = data[key]

        log.debug(item)
        response = self.table.put_item(Item=item)
        log.debug(response)

        post_status = entity_class.post(
            http_method="PATCH", data=data, existing_item=item
        )
        log.debug(f"post_status: {post_status}")
        return entity_class.filter_ddbkeys(item=item)

    def delete(self, *, entity_class: Entity, data: dict = {}) -> int:
        identity = entity_class.make_identity(data=data)
        log.debug(identity)
        response = self.table.get_item(Key=identity)
        if "Item" not in response:
            raise NotFound()
        item = entity_class.filter_ddbkeys(response["Item"])

        pre_status = entity_class.pre(http_method="DELETE", data=item)
        if pre_status is False:
            raise PreconditionFailed()

        response = self.table.delete_item(Key=identity)

        post_status = entity_class.post(http_method="DELETE", data=item)
        log.debug(f"post_status: {post_status}")
        return item
