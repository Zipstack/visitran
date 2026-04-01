from backend.core.routers.onboarding.views import OnboardingViewSet
from django.urls import path

onboarding_template = OnboardingViewSet.as_view(
    {
        "get": OnboardingViewSet.get_onboarding_template.__name__,
    }
)

onboarding_status = OnboardingViewSet.as_view(
    {
        "get": OnboardingViewSet.get_project_onboarding_status.__name__,
    }
)

onboarding_start = OnboardingViewSet.as_view(
    {
        "post": OnboardingViewSet.start_onboarding.__name__,
    }
)

onboarding_complete_task = OnboardingViewSet.as_view(
    {
        "post": OnboardingViewSet.complete_task.__name__,
    }
)

onboarding_skip_task = OnboardingViewSet.as_view(
    {
        "post": OnboardingViewSet.skip_task.__name__,
    }
)

onboarding_reset = OnboardingViewSet.as_view(
    {
        "post": OnboardingViewSet.reset_onboarding.__name__,
    }
)

onboarding_toggle = OnboardingViewSet.as_view(
    {
        "post": OnboardingViewSet.toggle_project_onboarding.__name__,
    }
)

onboarding_mark_complete = OnboardingViewSet.as_view(
    {
        "post": OnboardingViewSet.mark_complete.__name__,
    }
)

urlpatterns = [
    # Template management (no org required)
    path('templates/<str:template_id>/', onboarding_template, name='get_onboarding_template'),

    # Project-level onboarding management (org handled by middleware)
    path('status/', onboarding_status, name='get_project_onboarding_status'),
    path('start/', onboarding_start, name='start_onboarding'),
    path('complete-task/', onboarding_complete_task, name='complete_task'),
    path('skip-task/', onboarding_skip_task, name='skip_task'),
    path('mark-complete/', onboarding_mark_complete, name='mark_complete'),
    path('reset/', onboarding_reset, name='reset_onboarding'),
    path('toggle/', onboarding_toggle, name='toggle_project_onboarding'),
    # End of urlpatterns
]
