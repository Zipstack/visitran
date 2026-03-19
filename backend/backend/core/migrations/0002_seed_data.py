# Data migration for seeding initial data
# This migration seeds required ChatIntent and OnboardingTemplate records

from django.db import migrations
from uuid import uuid4


def seed_chat_intent(apps, schema_editor):
    """Seed required ChatIntent records."""
    ChatIntent = apps.get_model("core", "ChatIntent")

    required_intents = [
        {"name": "INFO", "display_name": "Chat"},
        {"name": "TRANSFORM", "display_name": "Transform"},
        {"name": "SQL", "display_name": "SQL"},
    ]

    for item in required_intents:
        ChatIntent.objects.get_or_create(
            name=item["name"],
            defaults={
                "chat_intent_id": uuid4(),
                "display_name": item["display_name"],
            }
        )


def seed_onboarding_templates(apps, schema_editor):
    """Seed initial onboarding templates."""
    OnboardingTemplate = apps.get_model('core', 'OnboardingTemplate')

    # Jaffle Shop template
    jaffle_template_data = {
        "title": "Welcome to Visitran AI",
        "description": "Explore the Jaffle Shop project and learn Visitran's key features. Complete these items in sequence.",
        "welcomeMessage": "Welcome to your Jaffle Shop starter project! This interactive onboarding will guide you through Visitran's key features using real data transformation tasks.",
        "items": [
            {
                "id": "task-1",
                "title": "Build a model to answer: What payment methods were used for various orders?",
                "description": "Create a transformation to analyze payment methods across orders.",
                "prompt": "Build a model to answer the question: What payment methods were used for various orders?",
                "mode": "transform"
            },
            {
                "id": "task-2",
                "title": "What is the customer lifetime value?",
                "description": "Create a transformation to calculate customer lifetime value.",
                "prompt": "Build a model to answer the question: What is the customer lifetime value?",
                "mode": "transform"
            },
            {
                "id": "task-3",
                "title": "Find unique payment methods customers can use",
                "description": "Write and run a SQL query to discover available payment methods.",
                "prompt": "Write and run a SQL query to find out the unique payment methods customers can use.",
                "mode": "sql"
            },
            {
                "id": "task-4",
                "title": "Describe transformations made by staging models",
                "description": "Get an explanation of the intermediate transformations.",
                "prompt": "Describe the transformations made by the intermediate / staging models.",
                "mode": "chat"
            }
        ]
    }

    # DVD Rental template
    dvd_template_data = {
        "title": "Welcome to Visitran AI - DVD Rental",
        "description": "Explore the DVD Rental project and learn Visitran's key features. Complete these items in sequence.",
        "welcomeMessage": "Welcome to your DVD Rental starter project! This interactive onboarding will guide you through Visitran's key features using real data transformation tasks.",
        "items": [
            {
                "id": "task-1",
                "title": "Build a model to answer: What are the top 10 most rented films?",
                "description": "Create a transformation to analyze rental data and find popular films.",
                "prompt": "Build a model to answer the question: What are the top 10 most rented films?",
                "mode": "transform"
            },
            {
                "id": "task-2",
                "title": "What is the average rental duration by category?",
                "description": "Create a transformation to calculate average rental duration grouped by film category.",
                "prompt": "Build a model to answer the question: What is the average rental duration by category?",
                "mode": "transform"
            },
            {
                "id": "task-3",
                "title": "Find customers who have never returned a film",
                "description": "Write and run a SQL query to identify customers with outstanding rentals.",
                "prompt": "Write and run a SQL query to find customers who have never returned a film.",
                "mode": "sql"
            },
            {
                "id": "task-4",
                "title": "Explain the customer segmentation model",
                "description": "Get an explanation of how customers are segmented in the data model.",
                "prompt": "Explain the customer segmentation model and how it categorizes different types of customers.",
                "mode": "chat"
            }
        ]
    }

    # Create templates if they don't exist
    OnboardingTemplate.objects.get_or_create(
        template_id='jaffleshop_starter',
        defaults={
            'title': 'Jaffle Shop Starter Onboarding',
            'description': 'Interactive onboarding for Jaffle Shop starter project',
            'welcome_message': 'Welcome to your Jaffle Shop starter project! This interactive onboarding will guide you through Visitran\'s key features using real data transformation tasks.',
            'template_data': jaffle_template_data,
            'is_active': True
        }
    )

    OnboardingTemplate.objects.get_or_create(
        template_id='dvd_starter',
        defaults={
            'title': 'DVD Starter Onboarding',
            'description': 'Interactive onboarding for DVD starter project',
            'welcome_message': 'Welcome to your DVD starter project! This interactive onboarding will guide you through Visitran\'s key features using real data transformation tasks.',
            'template_data': dvd_template_data,
            'is_active': True
        }
    )


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(seed_chat_intent, migrations.RunPython.noop),
        migrations.RunPython(seed_onboarding_templates, migrations.RunPython.noop),
    ]
