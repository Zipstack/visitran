from django.urls import path
from backend.core.routers.chat.views import ChatView

list_or_specific_chat = ChatView.as_view({'get': 'list_chats', 'post': 'persist_prompt'})
list_llm_models = ChatView.as_view({'get': 'list_llm_models'})
delete_chat = ChatView.as_view({'delete': 'delete_chat'})
update_chat = ChatView.as_view({'patch': 'update_chat_name'})

urlpatterns = [
    path('', list_or_specific_chat, name='list_or_specific_chat'),
    path('/delete/<str:chat_id>', delete_chat, name='delete_chat'),
    path('/update/<str:chat_id>', update_chat, name='update_chat'),
    path('/list-llm-models', list_llm_models, name='list_llm_models'),
]

