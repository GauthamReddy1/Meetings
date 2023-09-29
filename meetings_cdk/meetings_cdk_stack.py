from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as ddb,
    aws_apigateway as apigw,
    aws_lambda as _lambda,
    aws_ssm as ssm,
)
from constructs import Construct


class MeetingsCdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create DynamoDB table
        meetings = ddb.Table(
            self,
            "Meetings",
            partition_key=ddb.Attribute(name="id", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="sortKey", type=ddb.AttributeType.STRING),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            stream=ddb.StreamViewType.NEW_IMAGE,
        )

        meetings.add_global_secondary_index(
            index_name="UsernameUrlIndex",
            partition_key=ddb.Attribute(name="username", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="url", type=ddb.AttributeType.STRING),
        )

        # Create Lambda Layer

        my_layer = _lambda.LayerVersion(
            self,
            "MyLayer",
            code=_lambda.Code.from_asset("lambdas/layer"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
        )

        powertools_for_aws_lambda = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "powertools",
            "arn:aws:lambda:us-east-1:017000801446:layer:AWSLambdaPowertoolsPythonV2:45",
        )

        parameter_value = ssm.StringParameter.from_string_parameter_name(
            self, "IntegrationsAPI", "/api/integrations"
        ).string_value

        # Create Lambda functions
        def create_lambda_function(name: str):
            return _lambda.Function(
                self,
                f"{name}",
                runtime=_lambda.Runtime.PYTHON_3_11,
                handler=f"{name}.lambda_handler",
                code=_lambda.Code.from_asset(f"lambdas/"),
                environment={
                    "TABLE_NAME": meetings.table_name,
                    "INTEGRATIONS_API_URL": parameter_value,
                },
                layers=[my_layer, powertools_for_aws_lambda],
                timeout=Duration.seconds(30),
                memory_size=1400,
            )

        event_types_lambda = create_lambda_function("event-types")
        bookings_lambda = create_lambda_function("bookings")
        availabilities_lambda = create_lambda_function("availabilities")
        book_lambda = create_lambda_function("book")

        # Grant DynamoDB table permissions to Lambda functions
        meetings.grant_full_access(event_types_lambda)
        meetings.grant_full_access(bookings_lambda)
        meetings.grant_full_access(availabilities_lambda)
        meetings.grant_full_access(book_lambda)

        # Create API Gateway
        api = apigw.RestApi(
            self,
            "MyApi",
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS
            ),
        )

        validate_all = apigw.RequestValidator(
            self,
            "ValidateAll",
            rest_api=api,
            validate_request_body=True,
            validate_request_parameters=True,
        )

        # Availabilities Resource

        availability_model = apigw.Model(
            self,
            "AvailabilityModel",
            rest_api=api,
            schema=apigw.JsonSchema(
                schema=apigw.JsonSchemaVersion.DRAFT7,
                type=apigw.JsonSchemaType.OBJECT,
                properties={
                    "name": apigw.JsonSchema(type=apigw.JsonSchemaType.STRING),
                    "availabilities": apigw.JsonSchema(
                        type=apigw.JsonSchemaType.ARRAY,
                        items=apigw.JsonSchema(
                            type=apigw.JsonSchemaType.ARRAY,
                            items=apigw.JsonSchema(
                                type=apigw.JsonSchemaType.OBJECT,
                                properties={
                                    "start": apigw.JsonSchema(
                                        type=apigw.JsonSchemaType.STRING,
                                        pattern="^([0-1][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$",
                                    ),
                                    "end": apigw.JsonSchema(
                                        type=apigw.JsonSchemaType.STRING,
                                        pattern="^([0-1][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$",
                                    ),
                                },
                                required=["start", "end"],
                                additional_properties=False,
                            ),
                        ),
                        min_items=7,
                        max_items=7,
                    ),
                },
                required=["name", "availabilities"],
                additional_properties=False,
            ),
        )

        availabilities = api.root.add_resource("availabilities")
        availabilities.add_method(
            "GET",
            apigw.LambdaIntegration(availabilities_lambda),
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
            },
        )
        availabilities.add_method(
            "POST",
            apigw.LambdaIntegration(availabilities_lambda),
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
                "method.request.querystring.name": True,
            },
        )

        availabilities.add_method(
            "PUT",
            apigw.LambdaIntegration(availabilities_lambda),
            request_models={"application/json": availability_model},
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
                "method.request.querystring.availability_id": True,
            },
        )
        availabilities.add_method(
            "DELETE",
            apigw.LambdaIntegration(availabilities_lambda),
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
                "method.request.querystring.availability_id": True,
            },
        )

        # event-types

        event_type_model = apigw.Model(
            self,
            "EventTypeModel",
            rest_api=api,
            schema=apigw.JsonSchema(
                schema=apigw.JsonSchemaVersion.DRAFT7,
                type=apigw.JsonSchemaType.OBJECT,
                properties={
                    "name": apigw.JsonSchema(type=apigw.JsonSchemaType.STRING),
                    "description": apigw.JsonSchema(type=apigw.JsonSchemaType.STRING),
                    "url": apigw.JsonSchema(type=apigw.JsonSchemaType.STRING),
                    "duration": apigw.JsonSchema(type=apigw.JsonSchemaType.NUMBER),
                    "availability_id": apigw.JsonSchema(
                        type=apigw.JsonSchemaType.STRING
                    ),
                    "hidden": apigw.JsonSchema(type=apigw.JsonSchemaType.BOOLEAN),
                },
                required=[
                    "name",
                    "description",
                    "url",
                    "duration",
                    "availability_id",
                    "hidden",
                ],
                additional_properties=False,
            ),
        )

        event_type = api.root.add_resource("event-types")
        event_type.add_method(
            "GET",
            apigw.LambdaIntegration(event_types_lambda),
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
            },
        )
        event_type.add_method(
            "POST",
            apigw.LambdaIntegration(event_types_lambda),
            request_models={"application/json": event_type_model},
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
            },
        )

        event_type.add_method(
            "PUT",
            apigw.LambdaIntegration(event_types_lambda),
            request_models={"application/json": event_type_model},
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
                "method.request.querystring.event_type_id": True,
            },
        )
        event_type.add_method(
            "DELETE",
            apigw.LambdaIntegration(event_types_lambda),
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
                "method.request.querystring.event_type_id": True,
            },
        )

        # bookings
        booking_model = apigw.Model(
            self,
            "BookingModel",
            rest_api=api,
            schema=apigw.JsonSchema(
                schema=apigw.JsonSchemaVersion.DRAFT7,
                type=apigw.JsonSchemaType.OBJECT,
                properties={
                    "name": apigw.JsonSchema(type=apigw.JsonSchemaType.STRING),
                    "date": apigw.JsonSchema(
                        type=apigw.JsonSchemaType.STRING,
                        pattern="[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
                    ),
                    "host": apigw.JsonSchema(
                        type=apigw.JsonSchemaType.OBJECT,
                        properties={
                            "name": apigw.JsonSchema(type=apigw.JsonSchemaType.STRING),
                            "email": apigw.JsonSchema(type=apigw.JsonSchemaType.STRING),
                        },
                        required=["name", "email"],
                    ),
                    "guests": apigw.JsonSchema(
                        type=apigw.JsonSchemaType.ARRAY,
                        items=apigw.JsonSchema(
                            type=apigw.JsonSchemaType.OBJECT,
                            properties={
                                "name": apigw.JsonSchema(
                                    type=apigw.JsonSchemaType.STRING
                                ),
                                "email": apigw.JsonSchema(
                                    type=apigw.JsonSchemaType.STRING
                                ),
                            },
                            required=["name", "email"],
                        ),
                    ),
                },
                required=["name", "date", "host", "guests"],
                additional_properties=False,
            ),
        )

        bookings = api.root.add_resource("bookings")
        bookings.add_method(
            "GET",
            apigw.LambdaIntegration(bookings_lambda),
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
            },
        )
        bookings.add_method(
            "POST",
            apigw.LambdaIntegration(bookings_lambda),
            request_models={"application/json": booking_model},
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
            },
        )

        bookings.add_method(
            "PUT",
            apigw.LambdaIntegration(bookings_lambda),
            request_models={"application/json": booking_model},
            request_validator=validate_all,
            request_parameters={
                "method.request.querystring.id": True,
                "method.request.querystring.booking_id": True,
            },
        )

        # book

        username_resource = api.root.add_resource("book").add_resource("{username}")

        # Attach a GET method to book/{username}
        username_resource.add_method(
            "GET",
            apigw.LambdaIntegration(book_lambda),
        )

        # Create the nested resource book/{username}/{url}
        url_resource = username_resource.add_resource("{url}")

        # Attach a GET method to book/{username}/{url}
        url_resource.add_method(
            "GET",
            apigw.LambdaIntegration(book_lambda),
        )

        # Create the nested resource book/{username}/{url}/availabilities
        availabilities_resource = url_resource.add_resource("availabilities")

        availabilities_resource.add_method(
            "GET",
            apigw.LambdaIntegration(book_lambda),
        )

        # # Send Email After Event Success
        # email_lambda = _lambda.Function(
        #     self,
        #     "EmailLambda",
        #     runtime=_lambda.Runtime.PYTHON_3_11,
        #     handler="send_email.lambda_handler",
        #     code=_lambda.Code.from_asset("lambdas"),
        # )

        # # Grant the Lambda permissions to read from DynamoDB Stream
        # meetings.grant_stream_read(email_lambda)

        # # Grant the Lambda permissions to send emails using SES
        # email_lambda.add_to_role_policy(
        #     iam.PolicyStatement(
        #         actions=["ses:SendEmail", "ses:SendRawEmail"], resources=["*"]
        #     )
        # )

        # # Set the DynamoDB Stream as the Lambda trigger
        # _lambda.EventSourceMapping(
        #     self,
        #     "DynamoDBEventSource",
        #     target=email_lambda,
        #     event_source_arn=meetings.table_stream_arn,
        #     starting_position=_lambda.StartingPosition.TRIM_HORIZON,
        # )
