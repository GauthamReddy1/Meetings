from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler.api_gateway import (
    APIGatewayRestResolver,
    CORSConfig,
)
from src.book.book import (
    get_availabilities_for_url,
    get_event_type_for_url,
    get_event_types_for_username,
)

logger = Logger()

cors_config = CORSConfig(allow_origin="*")
app = APIGatewayRestResolver(cors=cors_config)


@app.get("/book/<username>")
def get_event_types(username: str):
    return get_event_types_for_username(username)


@app.get("/book/<username>/<url>")
def get_event_type(username: str, url: str):
    return get_event_type_for_url(username, url)


@app.get("/book/<username>/<url>/availabilities")
def get_availabilities(username: str, url: str):
    return get_availabilities_for_url(username, url)


def lambda_handler(event, context):
    return app.resolve(event, context)
