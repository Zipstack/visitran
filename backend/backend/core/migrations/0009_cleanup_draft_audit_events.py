# Data migration: remove audit events for deleted draft/conflict event types.

from django.db import migrations


def remove_draft_events(apps, schema_editor):
    VersionAuditEvent = apps.get_model("core", "VersionAuditEvent")
    VersionAuditEvent.objects.filter(
        event_type__in=[
            "draft_saved",
            "draft_discarded",
            "conflict_resolved",
            "conflict_finalized",
        ]
    ).delete()


def reverse_remove_draft_events(apps, schema_editor):
    pass  # irreversible data deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0008_remove_draft_conflict_tables"),
    ]

    operations = [
        migrations.RunPython(
            remove_draft_events,
            reverse_remove_draft_events,
        ),
    ]
