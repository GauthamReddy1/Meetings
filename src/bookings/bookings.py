import json
import boto3
import os
import uuid
from typing import Union, Optional
from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler.exceptions import InternalServerError

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])


logger = Logger()


def create(
    id: str, booking: dict[str, Union[str, list[str]]]
) -> dict[str, Union[int, str]]:
    """
    Create a new booking item in the table.

    Parameters:
    - id (str): The main identifier for the table item.
    - booking (dict): Contains the booking data to be stored.

    Returns:
    - dict: A dictionary containing a statusCode and a body message indicating the result of the operation.

    Raises:
    - Exception: Generic exception if there's an unexpected error during the creation operation.
    """
    try:
        sortKey = f"BOOKING:{uuid.uuid4()}"

        table.put_item(
            Item={
                "id": id,
                "sortKey": sortKey,
                "name": booking["name"],
                "date": booking["date"],
                "host": booking["host"],
                "guests": booking["guests"],
            },
            ConditionExpression="attribute_not_exists(id) AND attribute_not_exists(sortKey)",
        )

        logger.info(
            f"Booking item with ID {id} and sortKey {sortKey} created successfully."
        )
        return "Item created", 201

    except Exception as e:
        logger.exception(
            f"Error occurred while creating booking item with ID {id}: {str(e)}"
        )
        raise InternalServerError("Internal server error")


def convert_to_public_form(booking: dict[str, str]) -> Optional[dict[str, str]]:
    """
    Convert booking data to its public form.

    Parameters:
    - booking (dict): The original booking data.

    Returns:
    - dict or None: The booking data in its public form or None if an exception occurs.

    Note:
    - Exceptions will be logged using Lambda Powertools.
    """
    try:
        return {
            "owner": booking["id"],
            "booking_id": booking["sortKey"].split(":")[1],
            "data": booking["data"],
        }
    except Exception as e:
        logger.error(
            f"Error converting booking to public form. Exception: {str(e)}. Booking data: {booking}"
        )
        return None


def get(id: str) -> dict[str, Union[int, str]]:
    """
    Fetch booking data from the table based on the provided ID.

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
                ":prefix": "BOOKING:",
            },
        )

        items = response.get("Items", [])
        public_items = list(map(convert_to_public_form, items))
        logger.info(f"Fetched {len(public_items)} booking items for ID {id}.")

        return public_items

    except Exception as e:
        logger.exception(
            f"Error occurred while fetching booking items for ID {id}: {str(e)}. Original response: {response}"
        )
        raise InternalServerError("Internal server error")


def update(
    id: str, booking_id: str, booking: dict[str, Union[str, list[str]]]
) -> dict[str, Union[int, str]]:
    """
    Update booking data in the table based on given identifiers and booking data.

    Parameters:
    - id (str): The main identifier for the table item.
    - booking_id (str): The specific identifier for the booking data.
    - booking (dict): Contains updated booking data.

    Returns:
    - dict: A dictionary containing a statusCode and a body message indicating the result of the operation.

    Raises:
    - Exception: Generic exception if there's an unexpected error during the update operation.
    """
    try:
        sortKey = f"BOOKING:{booking_id}"

        table.update_item(
            Key={"id": id, "sortKey": sortKey},
            UpdateExpression="SET #name = :n, #date = :d, #host = :h, #guests = :g",
            ExpressionAttributeValues={
                ":n": booking["name"],
                ":d": booking["date"],
                ":h": booking["host"],
                ":g": booking["guests"],
            },
            ExpressionAttributeNames={
                "#name": "name",
                "#date": "date",
                "#host": "host",
                "#guests": "guests",
            },
        )

        logger.info(
            f"Booking item with ID {id} and sortKey {sortKey} updated successfully."
        )
        return "Item updated"

    except Exception as e:
        logger.exception(
            f"Error occurred while updating booking item with ID {id} and sortKey {sortKey}: {str(e)}"
        )
        raise InternalServerError("Internal server error")
