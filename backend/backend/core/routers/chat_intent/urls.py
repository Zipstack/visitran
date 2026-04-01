from django.urls import path

from backend.core.routers.chat_intent.views import ChatIntentView

list_chat_intents = ChatIntentView.as_view({"get": "list_chat_intents"})

urlpatterns = [
    path("", list_chat_intents, name="list_chat_intents"),
]
