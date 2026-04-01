from typing import List

from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from backend.utils.constants import HTTPMethods
from backend.core.utils import handle_http_request
from backend.application.context.application import ApplicationContext
from backend.application.config_parser.constants import AGGREGATE_DETAILS, AGGREGATE_DICT, FORMULA_DICT
from visitran.utils import get_adapter_connection_fields, get_adapters_list, import_file

from backend.core.user import UserService

from backend.utils.tenant_context import get_current_tenant

from backend.core.models.api_tokens import APIToken
from django.utils.timezone import now
from datetime import timedelta


@api_view([HTTPMethods.GET])
@handle_http_request
def get_datasource_fields(request: Request, datasource_name: str) -> Response:
    connection_fields = get_adapter_connection_fields(adapter_name=datasource_name)

    response = {
        "datasource_name": datasource_name,
        "datasource_field_details": connection_fields,
    }

    if "schema" in connection_fields.keys():
        response["is_schema_exists"] = True
    return Response(data=response, status=status.HTTP_200_OK)


@api_view([HTTPMethods.PUT])
@handle_http_request
def update_user_profile(request: Request) -> Response:
    user_service = UserService()
    user = user_service.get_user_by_email(request.user.email)
    user = user_service.update_user_display_names(user, request.data["first_name"], request.data["last_name"])
    update_user_token(request, user)
    return Response(
        data={
            'first_name': user.first_name,
            'last_name': user.last_name,
        },
        status=status.HTTP_200_OK
    )


def update_user_token(request, user):
    new_token = request.data.get("token")
    existing_token: APIToken = APIToken.objects.filter(user=user).first()

    if new_token:
        if existing_token and existing_token.token == new_token:
            return
        if existing_token:
            existing_token.delete()

        APIToken.objects.create(user=user, token=new_token, expires_at= now() + timedelta(days=90))
    else:
        if existing_token:
            existing_token.delete()


@api_view([HTTPMethods.GET])
@handle_http_request
def get_user_profile(request: Request) -> Response:
    user_service = UserService()
    user_data = user_service.get_user_by_email(request.user.email)
    token: APIToken = user_service.fetch_token(user=request.user)
    user_json = {
        'first_name': user_data.first_name,
        'last_name': user_data.last_name,
        'email': user_data.email,
        'userid': user_data.user_id,
        'profile_picture_url': user_data.profile_picture_url,
        "token": token.token if token else "",
        "token_expires_at": token.expires_at if token else "",
        "is_token_expired": not token.is_valid() if token else ""

    }
    return Response(data=user_json, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_datasource_list(request: Request) -> Response:
    """This method will return the list of adapters installed."""
    adapters_list: list[str] = get_adapters_list()
    
    # Soft delete: Remove Trino from the list
    if "trino" in adapters_list:
        adapters_list.remove("trino")
    
    data = []
    for adapter_name in adapters_list:
        icon = import_file(f"visitran.adapters.{adapter_name}").ICON
        data.append(
            {
                "value": adapter_name,
                "label": adapter_name.capitalize(),
                "icon": icon
            }
        )
    response_data = {
        "datasource": sorted(data, key=lambda x: x["value"]),
        "datasource_count": data.__len__(),
    }
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_aggregations_list(request: Request) -> Response:
    """This method will return the list of aggregations which are installed as
    a response."""
    aggregations_list: list[str] = AGGREGATE_DICT.keys()
    data = []
    for aggregation in aggregations_list:
        data.append(
            {
                "value": aggregation,
                "label": aggregation.upper(),
                "title": AGGREGATE_DETAILS.get(aggregation),
            }
        )
    response_data = {
        "aggregations": sorted(data, key=lambda x: x["value"]),
        "aggregation_count": data.__len__(),
    }
    return Response(data=response_data, status=status.HTTP_200_OK)


@api_view([HTTPMethods.GET])
@handle_http_request
def get_formula_list(request: Request) -> Response:
    """This method will return the list of FORMULA SQL which are supported as of
    now."""
    formula_details: list[str] = FORMULA_DICT.keys()
    _formula_response_data = []
    for formula_key, formula_value in FORMULA_DICT.items():
        _formula_response_data.append(
            {
                "value": formula_value.get("key"),
                "label": formula_value.get("key").capitalize(),
                "title": formula_value.get("description"),
            }
        )
    response_data = {
        "formulas": sorted(_formula_response_data, key=lambda x: x["value"]),
        "formula_list": sorted(formula_details),
        "formula_count": formula_details.__len__(),
    }
    return Response(data=response_data, status=status.HTTP_200_OK)

