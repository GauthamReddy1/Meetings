from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler.api_gateway import (
    APIGatewayRestResolver,
    CORSConfig,
)

from src.event_types.event_types import delete, get, update, create

logger = Logger()

cors_config = CORSConfig(allow_origin="*")
app = APIGatewayRestResolver(cors=cors_config)


@app.get("/event-types")
def get_event_types():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]

    return get(id)


@app.post("/event-types")
def create_event_type():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]
    event_type = app.current_event.json_body

    return create(id, event_type)


@app.put("/event-types")
def update_event_type():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]
    event_type_id = query_params["event_type_id"]
    event_type = app.current_event.json_body

    return update(id, event_type_id, event_type)


@app.delete("/event-types")
def delete_event_type():
    query_params = app.current_event.query_string_parameters
    id = query_params["id"]
    event_type_id = query_params["event_type_id"]

    return delete(id, event_type_id)


def lambda_handler(event, context):
    return app.resolve(event, context)
