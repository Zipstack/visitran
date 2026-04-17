from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0002_seed_data"),
    ]

    operations = [
        migrations.AddField(
            model_name="configmodels",
            name="run_status",
            field=models.CharField(
                choices=[
                    ("NOT_STARTED", "Not Started"),
                    ("RUNNING", "Running"),
                    ("SUCCESS", "Success"),
                    ("FAILED", "Failed"),
                ],
                default="NOT_STARTED",
                help_text="Current execution status of the model",
                max_length=20,
            ),
        ),
        migrations.AddField(
            model_name="configmodels",
            name="failure_reason",
            field=models.TextField(
                blank=True,
                help_text="Error message if the model execution failed",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="configmodels",
            name="last_run_at",
            field=models.DateTimeField(
                blank=True,
                help_text="Timestamp of the last execution",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="configmodels",
            name="run_duration",
            field=models.FloatField(
                blank=True,
                help_text="Duration of last execution in seconds",
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="projectdetails",
            name="project_schema",
            field=models.CharField(
                max_length=1024,
                blank=True,
                null=True,
            ),
        ),
    ]
