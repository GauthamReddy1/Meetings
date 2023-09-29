import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler.api_gateway import (
    APIGatewayRestResolver,
    CORSConfig,
)

from src.bookings.bookings import get, update, create

logger = Logger()

cors_config = CORSConfig(allow_origin="*")
app = APIGatewayRestResolver(cors=cors_config)


@app.get("/bookings")
def get_availabilities():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]

    return get(id)


@app.post("/bookings")
def create_availability():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]
    booking = app.current_event.json_body

    return create(id, booking)


@app.put("/bookings")
def update_availability():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]
    booking_id = query_params["booking_id"]
    booking = app.current_event.json_body

    return update(id, booking_id, booking)


def lambda_handler(event, context):
    return app.resolve(event, context)
