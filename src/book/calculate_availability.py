import json
import boto3
import os
from datetime import time, datetime, timedelta
from collections import defaultdict
from aws_lambda_powertools import Logger
from typing import Any, Tuple
import requests

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

INTEGRATIONS_API_URL = os.environ["INTEGRATIONS_API_URL"]
logger = Logger()


def get_availability(id: str, availability_id: str):
    """
    Fetch the availability data for a given ID and availability ID.

    Args:
        id: The primary ID associated with the availability.
        availability_id: The specific availability ID to retrieve.

    Returns:
        The availability data.

    Raises:
        NotFoundError: If no availability is found for the given IDs.
    """
    try:
        response = table.get_item(
            Key={"id": id, "sortKey": f"AVAILABILITY:{availability_id}"}
        )

        if "Item" not in response or "data" not in response["Item"]:
            raise Exception("No availability found for the given IDs.")

        return response["Item"]["data"]

    except Exception as e:
        logger.error(
            f"Error fetching availability for id {id} and availability_id {availability_id}. Error: {str(e)}"
        )
        raise e


def get_start_and_end_times() -> Tuple[datetime, datetime]:
    """
    Calculate the start and end times for event availability.
    Start time: Last Sunday from the current date.
    End time: The Saturday 6 weeks from the current date.

    Returns:
        A tuple containing the start and end times.
    """
    try:
        current_date = datetime.now()

        # Calculate start time: last Sunday from the current date
        days_since_sunday = current_date.weekday() + 1
        start_time = current_date - timedelta(days=days_since_sunday)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)

        # Calculate end time: The Saturday 6 weeks from the current date
        end_time = current_date + timedelta(weeks=6)
        days_to_next_saturday = 5 - current_date.weekday()
        end_time += timedelta(days=days_to_next_saturday)
        end_time = end_time.replace(hour=23, minute=59, second=59, microsecond=999999)

        return start_time, end_time

    except Exception as e:
        logger.error(f"Error calculating start and end times. Error: {str(e)}")
        raise e


def within_working_hours(
    working_hours: list[dict[str, str]],
    time_slot_start: datetime,
    time_slot_end: datetime,
) -> bool:
    """
    Check if a given time slot falls within the provided working hours.

    Args:
        working_hours: A list of dictionaries representing working hours.
                       Each dictionary has 'start' and 'end' keys with time values in ISO format.
        time_slot_start: The start time of the time slot.
        time_slot_end: The end time of the time slot.

    Returns:
        True if the time slot falls within the working hours, False otherwise.
    """
    try:
        for time_range in working_hours:
            time_range_start = time.fromisoformat(time_range["start"])
            time_range_end = time.fromisoformat(time_range["end"])

            if (
                time_range_start <= time_slot_start.time() < time_range_end
                and time_range_start <= time_slot_end.time() <= time_range_end
            ):
                return True

        return False

    except Exception as e:
        logger.error(f"Error checking time slot within working hours. Error: {str(e)}")
        raise e


def overlapping_with_event(
    events: list[dict[str, str]], time_slot_start: datetime, time_slot_end: datetime
) -> bool:
    """
    Check if a given time slot overlaps with any of the provided events.

    Args:
        events: A list of dictionaries representing events.
                Each dictionary has 'start' and 'end' keys with datetime values in ISO format.
        time_slot_start: The start time of the time slot.
        time_slot_end: The end time of the time slot.

    Returns:
        True if the time slot overlaps with any event, False otherwise.
    """
    try:
        for event in events:
            event_start = datetime.fromisoformat(event["start"])
            event_end = datetime.fromisoformat(event["end"])

            if (
                (time_slot_start >= event_start and time_slot_start < event_end)
                or (time_slot_end > event_start and time_slot_end <= event_end)
                or (time_slot_start < event_start and time_slot_end > event_end)
            ):
                return True

        return False

    except Exception as e:
        logger.error(
            f"Error checking overlap with events. Error: {str(e)}. Events: {events}"
        )
        raise e


def get_availabilities(event_type: dict[str, Any]) -> dict[str, Any]:
    """
    Get available time slots for a given event type.

    Args:
        event_type: Dictionary with event type details, specifically 'id' and 'availability_id'.

    Returns:
        A dictionary with status code and free time slots in the body.
    """
    try:
        availability_data = get_availability(
            event_type["id"], event_type["availability_id"]
        )
        start_time, end_time = get_start_and_end_times()

        freeTimes = defaultdict(list)

        response = requests.get(
            f"{INTEGRATIONS_API_URL}/events?start_time={start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}&end_time={end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}&id={event_type['id']}"
        )
        events = response.json()["events"]

        duration_seconds = 15 * 60  # Assuming duration is in minutes

        time_slot_start = start_time
        time_slot_end = time_slot_start + timedelta(seconds=duration_seconds)

        first_loop = True
        while time_slot_end <= end_time:
            if not first_loop:
                time_slot_start += timedelta(seconds=duration_seconds)
                time_slot_end += timedelta(seconds=duration_seconds)
            else:
                first_loop = False

            working_hours = availability_data[time_slot_start.weekday()]

            if not within_working_hours(working_hours, time_slot_start, time_slot_end):
                continue

            if overlapping_with_event(events, time_slot_start, time_slot_end):
                continue

            # Extract date from start_time_obj
            date_str = time_slot_start.strftime("%Y-%m-%d")

            # Create the transformed slot data
            transformed_slot = {
                "time": time_slot_start.isoformat(),
                "users": [],
            }
            # Directly append to the defaultdict
            freeTimes[date_str].append(transformed_slot)

        return {"statusCode": 200, "body": json.dumps(dict(freeTimes))}

    except Exception as e:
        logger.error(
            f"Error getting availabilities for event type {event_type}. Error: {str(e)}"
        )
        raise e
