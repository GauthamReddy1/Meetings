import uuid
import boto3
import os
from typing import Tuple, Union, Optional
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler.exceptions import (
    InternalServerError,
    NotFoundError,
)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

logger = Logger()


def convert_to_public_form(
    event_type: dict[str, Union[str, int, bool]]
) -> Optional[dict[str, Union[str, int, bool]]]:
    """
    Convert event type data to a public form.

    Parameters:
    - event_type (dict): Contains the raw event type data.

    Returns:
    - dict or None: The event type in the desired public format or None if there's an exception during conversion.

    Note:
    If any exception occurs during conversion, this function will log the error and return None.
    """
    try:
        return {
            "owner": event_type["id"],
            "event_type_id": event_type["sortKey"].split(":")[1],
            "name": event_type["name"],
            "description": event_type["description"],
            "url": event_type["url"],
            "duration": int(event_type["duration"]),
            "availability_id": event_type["availability_id"],
            "hidden": event_type["hidden"],
            "username": event_type["username"],
        }
    except Exception as e:
        logger.exception(
            f"Error occurred while converting event type to public form: {str(e)} - Event type data: {event_type}"
        )
        return None


def get(id: str) -> list[dict[str, Union[str, int, bool]]]:
    """
    Retrieve event items from the table based on the given identifier.

    Parameters:
    - id (str): The main identifier for the table items.

    Returns:
    - list[dict]: A list of event items in the desired public format.

    Note:
    If any exception occurs during retrieval, this function will log the error.
    """
    try:
        response = table.query(
            KeyConditionExpression="id = :id AND begins_with(sortKey, :prefix)",
            ExpressionAttributeValues={
                ":id": id,
                ":prefix": "EVENT:",
            },
        )

        items = response.get("Items", [])
        public_items = list(map(convert_to_public_form, items))

        logger.info(f"Retrieved {len(public_items)} event items for ID {id}.")
        return public_items

    except Exception as e:
        logger.exception(
            f"Unexpected error occurred while retrieving event items for ID {id}: {str(e)}"
        )
    raise InternalServerError("Internal server error")


def create(id: str, event_type: dict[str, Union[str, int, bool]]) -> Tuple[str, int]:
    """
    Create a new event item in the table.

    Parameters:
    - id (str): The main identifier for the table item.
    - event_type (dict): Contains the event type data to be stored.

    Returns:
    - Tuple[str, int]: A tuple containing a message string and a statusCode indicating the result of the operation.

    Raises:
    - ItemAlreadyExistsError: When trying to create an item that already exists.
    - InternalServerError: When a non-conditional error occurs.
    """
    try:
        sortKey = f"EVENT:{uuid.uuid4()}"

        table.put_item(
            Item={
                "id": id,
                "sortKey": sortKey,
                "name": event_type["name"],
                "description": event_type["description"],
                "url": event_type["url"],
                "duration": event_type["duration"],
                "availability_id": event_type["availability_id"],
                "hidden": event_type["hidden"],
                "username": id,
            },
        )

        logger.info(
            f"Event_Type with ID {id} and sortKey {sortKey} created successfully."
        )
        return "Item created", 201

    except Exception as e:
        logger.exception(
            f"Unexpected error occurred while creating event item with ID {id}: {str(e)}"
        )
        raise InternalServerError("Internal server error")


def update(
    id: str, event_type_id: str, event_type: dict[str, Union[str, int, bool]]
) -> str:
    """
    Update an event item in the table based on given identifiers and event type data.

    Parameters:
    - id (str): The main identifier for the table item.
    - event_type_id (str): The specific identifier for the event type data.
    - event_type (dict): Contains the updated event type data.

    Returns:
    - str: A message string indicating the result of the operation.

    Raises:
    - ItemNotFoundError: When the specified item is not found in the table.
    - InternalServerError: When a non-conditional error occurs.
    """
    try:
        sortKey = f"EVENT:{event_type_id}"

        table.update_item(
            Key={"id": id, "sortKey": sortKey},
            UpdateExpression="SET #name = :n, #description = :d, #url = :url, #duration = :dur, #availability_id = :id, #hidden = :h",
            ExpressionAttributeValues={
                ":n": event_type["name"],
                ":d": event_type["description"],
                ":url": event_type["url"],
                ":dur": event_type["duration"],
                ":id": event_type["availability_id"],
                ":h": event_type["hidden"],
            },
            ExpressionAttributeNames={
                "#name": "name",
                "#description": "description",
                "#url": "url",
                "#duration": "duration",
                "#availability_id": "availability_id",
                "#hidden": "hidden",
            },
            ConditionExpression="attribute_exists(id) AND attribute_exists(sortKey)",
        )

        logger.info(
            f"Event item with ID {id} and sortKey {sortKey} updated successfully."
        )
        return "Item updated"

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.error(
                f"Item with ID {id} and sortKey {sortKey} not found. Failed to update."
            )
            raise NotFoundError(
                f"Event type with event_type_id {event_type_id} does not exist."
            )
        else:
            logger.error(f"Error occurred during update operation: {str(e)}")
            raise InternalServerError("Internal server error")

    except Exception as e:
        logger.exception(
            f"Unexpected error occurred while updating event item with ID {id} and sortKey {sortKey}: {str(e)}"
        )
        raise InternalServerError("Internal server error")


def delete(id: str, event_type_id: str) -> str:
    """
    Delete an event type from the table based on given identifiers.

    Parameters:
    - id (str): The main identifier for the table item.
    - event_type_id (str): The specific identifier for the event type data.

    Returns:
    - str: A message string indicating the result of the operation.

    Raises:
    - ItemNotFoundError: When the specified item is not found in the table.
    - InternalServerError: When a non-conditional error occurs.
    """
    try:
        sortKey = f"EVENT:{event_type_id}"

        table.delete_item(
            Key={"id": id, "sortKey": sortKey},
            ConditionExpression="attribute_exists(id) AND attribute_exists(sortKey)",
        )

        logger.info(
            f"Event type with ID {id} and sortKey {sortKey} deleted successfully."
        )
        return "Item deleted"

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.error(
                f"Item with ID {id} and sortKey {sortKey} not found. Failed to delete."
            )
            raise NotFoundError(
                f"Event type with event_type_id {event_type_id} does not exist."
            )
        else:
            logger.error(f"Error occurred during delete operation: {str(e)}")
            raise InternalServerError("Internal server error")

    except Exception as e:
        logger.exception(
            f"Unexpected error occurred while deleting event type with ID {id} and sortKey {sortKey}: {str(e)}"
        )
        raise InternalServerError("Internal server error")
