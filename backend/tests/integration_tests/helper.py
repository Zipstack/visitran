from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from django.core.wsgi import get_wsgi_application
from django.test import Client
from rest_framework.response import Response

if TYPE_CHECKING:  # pragma: no cover
    pass

PORT = 8000
TEST_PROJECT_NAME = "duckdbuitest"
jsontype = "application/json"
CONTENT_JSON = {"Content-Type": jsontype}


class RequestFactory:
    """A class to create HTTP requests."""

    def __init__(self) -> None:
        """Initializes the RequestFactory object with a base URL."""
        os.environ["DJANGO_SETTINGS_MODULE"] = "server.settings.dev"
        get_wsgi_application()
        self.client = Client()
        self.base_url = "/api/v1/"

    def __call__(self, endpoint: str, raw: bool = False) -> Response:
        """Sends a GET request to the specified endpoint and returns the
        response."""
        full_url = self.base_url + endpoint
        response = self.client.get(full_url)
        if raw:
            return response
        assert (
            response.status_code == 200
        ), f"Request to {endpoint} failed with status code {response.status_code},\
            error: {response}"
        assert (
            response.headers.get("content-type") == jsontype
        ), f"Request to {endpoint} failed with content-type \
            {response.headers.get('content-type')}"
        return response

    def put(
        self,
        endpoint: str,
        payload: Any = None,
        headers: Any = None,
    ) -> Response:
        """Sends a PUT request to the specified endpoint with the given payload
        and returns the response."""
        full_url = self.base_url + endpoint
        response = self.client.put(full_url, headers=headers, data=payload)
        assert (
            response.status_code == 201
        ), f"Request to {endpoint} failed with status code {response.status_code},\
            error: {response}"
        assert (
            response.headers.get("content-type") == jsontype
        ), f"Request to {endpoint} failed with content-type \
            {response.headers.get('content-type')}"
        return response

    def post(
        self,
        endpoint: str,
        payload: Any = None,
        content_type: str = jsontype,
        return_response: bool = False,
    ) -> Response:
        """Sends a POST request to the specified endpoint with the given
        payload and returns the response."""
        full_url = self.base_url + endpoint

        if content_type == "multipart":
            response = self.client.post(full_url, data=payload, format="multipart")
        else:
            response = self.client.post(full_url, data=payload, content_type=content_type)

        if return_response:
            return response
        assert (
            response.status_code == 200
        ), f"Request to {endpoint} failed with status code {response.status_code},\
              error: {response}"
        assert (
            response.headers.get("content-type") == jsontype
        ), f"Request to {endpoint} failed with content-type \
            {response.headers.get('content-type')}"
        return response

    def delete(
        self,
        endpoint: str,
        payload: Any = None,
        headers: Any = None,
    ) -> Response:
        """Sends a DELETE request to the specified endpoint with the given."""
        full_url = self.base_url + endpoint
        response = self.client.delete(full_url, headers=headers, data=payload)
        assert (
            response.status_code == 200
        ), f"Request to {endpoint} failed with status code {response.status_code},\
              error: {response}"
        assert (
            response.headers.get("content-type") == jsontype
        ), f"Request to {endpoint} failed with content-type \
            {response.headers.get('content-type')}"
        return response
