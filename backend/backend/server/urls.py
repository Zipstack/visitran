"""Server URL Configuration.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from importlib.util import find_spec

from django.conf import settings
from django.conf.urls import include
from django.contrib.auth.forms import AuthenticationForm
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.urls import path, re_path

AuthenticationForm.environment = settings.DEBUG
from backend.core.urls import urlpatterns as core_urls


def render_react(request: HttpRequest) -> HttpResponse:
    return render(request, "index.html")


urlpatterns = [
    path("", include(core_urls)),
    path("", include("pluggable_apps.tenant_account.urls")),
    re_path(r"^$", render_react),
    re_path(r"^(?:.*)/?$", render_react),
]
if find_spec("pluggable_apps.tenant_account.urls"):
    ACCOUNT_URL = path("", include("pluggable_apps.tenant_account.urls"))
    urlpatterns.append(ACCOUNT_URL)
