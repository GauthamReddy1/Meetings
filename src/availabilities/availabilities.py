import os
import boto3
import uuid
from typing import Any, Union, Optional
from botocore.exceptions import ClientError
from datetime import time
from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler.exceptions import (
    InternalServerError,
    NotFoundError,
)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

logger = Logger()


def convert_to_public_form(availability: dict[str, str]) -> Optional[dict[str, str]]:
    """
    Convert availability data to its public form.

    Parameters:
    - availability (dict): The original availability data.

    Returns:
    - dict or None: The availability data in its public form or None if an exception occurs.

    Note:
    - Exceptions will be logged using Lambda Powertools.
    """
    try:
        return {
            "owner": availability["id"],
            "availability_id": availability["sortKey"].split(":")[1],
            "name": availability["name"],
            "data": availability["data"],
            "timezone": availability["timezone"],
        }
    except Exception as e:
        logger.error(
            f"Error converting availability to public form. Exception: {str(e)}. Availability data: {availability}"
        )
        return None


def get(id: str) -> dict[str, Union[int, str]]:
    """
    Fetch availability data from the table based on the provided ID.

    Parameters:
    - id (str): The main identifier for the table item.

    Returns:
    - dict: A dictionary containing a statusCode and a body message indicating the fetched items.

    Raises:
    - Exception: Generic exception if there's an unexpected error during the fetch operation.
    """
    try:
        response = table.query(
            KeyConditionExpression="id = :id AND begins_with(sortKey, :prefix)",
            ExpressionAttributeValues={
                ":id": id,
                ":prefix": "AVAILABILITY:",
            },
        )

        items = response.get("Items", [])
        public_items = list(map(convert_to_public_form, items))
        logger.info(f"Fetched {len(public_items)} items for ID {id}.")

        return public_items

    except Exception as e:
        logger.exception(f"Error occurred while fetching items for ID {id}: {str(e)}")
        raise InternalServerError("Internal server error")


def default_availability() -> list[list[dict[str, str]]]:
    """
    Generate a default weekly availability.

    The default availability is:
    - From 09:00 to 17:00 on weekdays.
    - No availability on weekends.

    Returns:
    - List[List[dict[str, str]]]: A weekly availability list containing daily availability slots.
    """
    availability = []
    start = time(9, 0, 0).isoformat()
    end = time(17, 0, 0).isoformat()
    weekday_slot = [{"start": start, "end": end}]
    weekend_slot = []

    for day in range(7):
        if day == 0 or day == 6:  # Sunday or Saturday
            availability.append(weekend_slot)
        else:
            availability.append(weekday_slot)

    return availability


def create(id: str, name: str) -> dict[str, Union[int, str]]:
    """
    Create a new availability item in the table.

    Parameters:
    - id (str): The main identifier for the table item.
    - name (str): Name of the availability.

    Returns:
    - dict: A dictionary containing a statusCode and a body message indicating the result of the operation.

    Raises:
    - Exception: Generic exception if there's an unexpected error during the creation operation.
    """
    try:
        sortKey = f"AVAILABILITY:{uuid.uuid4()}"

        table.put_item(
            Item={
                "id": id,
                "sortKey": sortKey,
                "name": name,
                "data": default_availability(),
                "timezone": "America/New_York",
            }
        )

        logger.info(f"Item with ID {id} and sortKey {sortKey} created successfully.")
        return "Item created", 201

    except Exception as e:
        logger.exception(
            f"Error occurred while creating availability for user with ID {id}: {str(e)}"
        )
        raise InternalServerError("Internal server error")


def update(
    id: str, availability_id: str, availability: dict[str, Any]
) -> dict[str, Union[int, str]]:
    """
    Update availability data in the table.

    Parameters:
    - id (str): The main identifier for the table item.
    - availability_id (str): The specific identifier for the availability data.
    - availability (dict): Contains updated availability data.

    Returns:
    - dict: A dictionary containing a statusCode and a body message indicating the result of the operation.

    Raises:
    - Exception: Generic exception if there's an unexpected error during the update operation.
    """
    try:
        sortKey = f"AVAILABILITY:{availability_id}"

        table.update_item(
            Key={"id": id, "sortKey": sortKey},
            UpdateExpression="SET #data = :d, #name = :n",
            ExpressionAttributeValues={
                ":d": availability["availabilities"],
                ":n": availability["name"],
            },
            ExpressionAttributeNames={"#data": "data", "#name": "name"},
            ConditionExpression="attribute_exists(id) AND attribute_exists(sortKey)",
        )

        logger.info(f"Item with ID {id} and sortKey {sortKey} updated successfully.")
        return "Item updated"

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.error(
                f"Availability with ID {id} and sortKey {sortKey} not found. Failed to update."
            )
            raise NotFoundError(
                f"Availability with availability_id {availability_id} does not exist."
            )
        else:
            logger.error(f"Error occurred during update operation: {str(e)}")
            raise InternalServerError("Internal server error")

    except Exception as e:
        logger.exception(
            f"Error occurred while updating item with ID {id} and sortKey {sortKey}: {str(e)}"
        )
        raise InternalServerError("Internal server error")


def delete(id: str, availability_id: str) -> dict[str, Union[int, str]]:
    """
    Delete availability data from the table based on given identifiers.

    Parameters:
    - id (str): The main identifier for the table item.
    - availability_id (str): The specific identifier for the availability data.

    Returns:
    - dict: A dictionary containing a statusCode and a body message indicating the result of the operation.

    Raises:
    - NotFoundError: When the specified item is not found in the table.
    - InternalServerError: When a non-conditional error occurs.
    """
    try:
        sortKey = f"AVAILABILITY:{availability_id}"

        table.delete_item(
            Key={"id": id, "sortKey": sortKey},
            ConditionExpression="attribute_exists(id) AND attribute_exists(sortKey)",
        )

        logger.info(f"Item with ID {id} and sortKey {sortKey} deleted successfully.")
        return "Item deleted"

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.error(
                f"Item with ID {id} and sortKey {sortKey} not found. Failed to delete."
            )
            raise NotFoundError(
                f"Availability with availability_id {availability_id} does not exist."
            )
        else:
            logger.error(f"Error occurred during delete operation: {str(e)}")
            raise InternalServerError("Internal server error")

    except Exception as e:
        logger.exception(
            f"Unexpected error occurred while deleting item with ID {id} and sortKey {sortKey}: {str(e)}"
        )
        raise InternalServerError("Internal server error")
