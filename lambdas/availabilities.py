import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler.api_gateway import (
    APIGatewayRestResolver,
    CORSConfig,
)

from src.availabilities.availabilities import delete, get, update, create

logger = Logger()

cors_config = CORSConfig(allow_origin="*")
app = APIGatewayRestResolver(cors=cors_config)


@app.get("/availabilities")
def get_availabilities():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]

    return get(id)


@app.post("/availabilities")
def create_availability():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]
    name = query_params["name"]

    return create(id, name)


@app.put("/availabilities")
def update_availability():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]
    availability_id = query_params["availability_id"]
    availability = app.current_event.json_body

    return update(id, availability_id, availability)


@app.delete("/availabilities")
def delete_availability():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]
    availability_id = query_params["availability_id"]

    return delete(id, availability_id)


def lambda_handler(event, context):
    return app.resolve(event, context)
