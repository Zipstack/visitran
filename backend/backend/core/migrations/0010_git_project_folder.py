"""Add git_project_folder to GitRepoConfig for branch import feature."""

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0009_cleanup_draft_audit_events"),
    ]

    operations = [
        migrations.AddField(
            model_name="gitrepoconfig",
            name="git_project_folder",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
    ]
