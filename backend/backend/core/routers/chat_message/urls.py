from django.urls import path
from backend.core.routers.chat_message.views.message_views import ChatMessageView
from backend.core.routers.chat_message.views.feedback_views import ChatMessageFeedbackView

chat_messages = ChatMessageView.as_view({'get': 'list_messages', 'post': 'persist_prompt'})
token_usage = ChatMessageView.as_view({'get': 'get_token_usage'})

urlpatterns = [
    path('', chat_messages, name='chat_messages'),
    path('/<uuid:chat_message_id>/feedback/', ChatMessageFeedbackView.as_view(), name='chat_message_feedback'),
    path('/<uuid:chat_message_id>/token-usage/', token_usage, name='chat_message_token_usage'),
]
