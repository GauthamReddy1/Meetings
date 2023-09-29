from typing import Union, Optional, Any
import boto3
import os
import json

from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler.exceptions import (
    InternalServerError,
    NotFoundError,
)

from .calculate_availability import get_availabilities

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

logger = Logger()


def convert_to_public_form(
    event: dict[str, Union[str, int]], filter_hidden: bool = True
) -> Optional[dict[str, Union[str, int]]]:
    """
    Convert raw event data to a public form.

    Parameters:
    - event (dict): Contains the raw event data.

    Returns:
    - dict or None: The event in the desired public format or None if there's an exception during conversion.

    Note:
    If any exception occurs during conversion, this function will log the error and return None.
    """
    try:
        if event["url"] == "RESERVED_FOR_INTERNAL_USE_ONLY" or (
            filter_hidden and event["hidden"]
        ):
            return None
        return {
            "name": event["name"],
            "description": event["description"],
            "duration": int(event["duration"]),
        }
    except Exception as e:
        logger.exception(
            f"Error occurred while converting event to public form: {str(e)} - Event data: {event}"
        )
        return None


def get_event_types_for_username(username: str) -> list[dict[str, Any]]:
    """
    Fetch event types for a given username.

    Args:
        username: The username to query event types for.

    Returns:
        A list of public event items.

    Raises:
        NotFoundError: If no event types are found for the username.
    """
    try:
        # First query without filtering hidden attribute
        response = table.query(
            IndexName="UsernameUrlIndex",
            KeyConditionExpression="#username = :username",
            ExpressionAttributeValues={":username": username},
            ExpressionAttributeNames={"#username": "username"},
        )
        items = response.get("Items", [])

        # If the list is empty, raise NotFoundError
        if not items:
            logger.warning(
                f"User with {username} does not exist or does not have RESERVED_FOR_INTERNAL_USE_ONLY url row"
            )
            raise NotFoundError(f"User with username {username} does not exist")

        public_items = list(map(convert_to_public_form, items))
        return public_items
    except NotFoundError as e:
        raise e
    except Exception as e:
        logger.error(f"Error fetching event types for username {username}. Error: {str(e)}")
        raise InternalServerError("Internal service error")


def get_event_type_for_url(
    username: str, url: str, public_use: bool = True
) -> dict[str, Any]:
    """
    Fetch an event type for a given username and URL.

    Args:
        username: The username associated with the event type.
        url: The URL of the event type.

    Returns:
        A dictionary representing the event item.

    Raises:
        NotFoundError: If no event type is found for the given username and URL.
    """
    if url == "RESERVED_FOR_INTERNAL_USE_ONLY":
        logger.warning(f"Reserved URL used for username {username}")
        raise NotFoundError("Reserved URL used.")

    try:
        response = table.query(
            IndexName="UsernameUrlIndex",
            KeyConditionExpression="#username = :username AND #url = :url",
            ExpressionAttributeValues={":username": username, ":url": url},
            ExpressionAttributeNames={
                "#username": "username",
                "#url": "url",
            },
        )
        items = response.get("Items", [])

        if not items:
            raise NotFoundError("No event type found for the given username and URL.")
        elif len(items) > 1:
            logger.error(
                f"Multiple event types found for username {username} and URL {url}. Returning the first item."
            )
            return convert_to_public_form(items[0]) if public_use else items[0]
        else:
            return convert_to_public_form(items[0]) if public_use else items[0]
    except NotFoundError as e:
        raise e
    except Exception as e:
        logger.error(
            f"Error fetching event type for username {username} and URL {url}. Error: {str(e)}"
        )
        raise InternalServerError("Internal service error")


def get_availabilities_for_url(username: str, url: str) -> dict[str, Any]:
    """
    Get availabilities for a given username and URL.

    Args:
        username: The username associated with the event type.
        url: The URL of the event type.

    Returns:
        A dictionary with availability details.

    Raises:
        NotFoundError: If no event type is found for the given username and URL.
        InternalServerError: If there's any unexpected error while processing.
    """
    try:
        logger.info("hello")
        event_type = get_event_type_for_url(username, url, public_use=False)
        logger.info(event_type)
        return get_availabilities(event_type)
    except NotFoundError as e:
        raise e
    except Exception as e:
        logger.error(
            f"Error fetching availabilities for event_type for username {username} and URL {url}. Error: {str(e)}"
        )
        raise InternalServerError("Internal service error")
