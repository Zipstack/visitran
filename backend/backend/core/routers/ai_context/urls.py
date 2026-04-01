from django.urls import path
from . import views

urlpatterns = [
    # Personal AI Context Rules
    path('user/ai-context-rules/', views.user_ai_context_rules, name='user-ai-context-rules'),
    
    # Project AI Context Rules
    path('project/<str:project_id>/ai-context-rules/', views.project_ai_context_rules, name='project-ai-context-rules'),
]
