import os

import boto3
from dynamoer.entity import Entity
from dynamoer.utils import log


class DynamoDbProxy(object):
    def __init__(self, *, table_name: str):
        session = boto3.session.Session()
        self.dynamodb = session.resource(
            "dynamodb", endpoint_url=os.getenv("DDB_ENDPOINT")
        )
        self.table_name = table_name
        self.table = self.dynamodb.Table(table_name)

    def get_item(
        self,
        *,
        entity_class: Entity,
        data: dict = {},
        admin: bool = False,
        cognito_groups: list = [],
    ) -> list:
        response = entity_class.query(
            data=data, table=self.table, admin=admin, cognito_groups=cognito_groups,
        )
        if "Item" in response:
            return entity_class.filter_ddbkeys(response["Item"])
        elif "Items" in response:
            return entity_class.filter_ddbkeys_list(response["Items"])
        else:
            raise Exception("Not Found.")

    def post_item(self, *, entity_class: Entity, data: dict = {}) -> dict:
        instance = entity_class(
            data=data
        )  # raise ValueError if data is not valid for given entity
        identity = instance.identity()
        log.debug(identity)
        try:
            response = self.table.get_item(Key=identity)
        except Exception as e:
            log.error(e)
            raise e
        log.debug(response)
        if "Item" in response:
            log.debug("Already exists.")
            raise Exception(f"Already exists.")
        try:
            item = instance.data_with_identity()
            log.debug(item)
            pre_status = entity_class.pre(http_method="POST", data=item)
            if pre_status is False:
                raise Exception(f"Pre-condition failed")
            response = self.table.put_item(Item=item)
            log.debug(response)
            post_status = instance.post(http_method="POST", data=item)
            log.debug(f"post-condition status: {post_status}")
            return instance.data()
        except Exception as e:
            log.error(f"post_item failed {e}")
            raise Exception(e)

    def put_item(self, *, entity_class: Entity, data: dict = {}) -> int:
        instance = entity_class(
            data=data
        )  # raise ValueError if data is not valid for given entity
        identity = instance.identity()
        log.debug(identity)
        response = self.table.get_item(Key=identity)
        log.debug(response)
        if "Item" not in response:
            log.debug("Not found")
            raise Exception(f"Not found.")
        item = instance.data_with_identity()
        log.debug(item)

        pre_status = entity_class.pre(
            http_method="PUT", data=item, existing_item=response["Item"]
        )
        if pre_status is False:
            raise Exception(f"Pre-condition failed")

        response = self.table.put_item(Item=item)
        log.debug(response)
        return instance.data()

    def patch_item(self, *, entity_class: Entity, data: dict = {}) -> int:
        identity = entity_class.make_identity(data=data)
        log.debug(identity)
        response = self.table.get_item(Key=identity)
        log.debug(response)
        if "Item" not in response:
            log.debug("Not found")
            raise Exception(f"Not found.")
        item = response["Item"]
        pre_status = entity_class.pre(
            http_method="PATCH", data=data, existing_item=item
        )
        if pre_status is False:
            raise Exception(f"Pre-condition failed")

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

    def delete_item(self, *, entity_class: Entity, data: dict = {}) -> int:
        identity = entity_class.make_identity(data=data)
        log.debug(identity)
        response = self.table.get_item(Key=identity)
        if "Item" not in response:
            raise Exception(f"Not found: {identity}")  # http 404
        item = entity_class.filter_ddbkeys(response["Item"])

        pre_status = entity_class.pre(http_method="DELETE", data=item)
        if pre_status is False:
            raise Exception(f"pre-condition failed")

        response = self.table.delete_item(Key=identity)

        post_status = entity_class.post(http_method="DELETE", data=item)
        log.debug(f"post_status: {post_status}")
        return item
