from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("job_scheduler", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="taskrunhistory",
            name="trigger",
            field=models.CharField(
                choices=[("scheduled", "Scheduled"), ("manual", "Manual")],
                default="scheduled",
                help_text="How the run was initiated: cron/interval schedule or manual dispatch.",
                max_length=20,
            ),
        ),
        migrations.AddField(
            model_name="taskrunhistory",
            name="scope",
            field=models.CharField(
                choices=[("job", "Full job"), ("model", "Single model")],
                default="job",
                help_text="Whether the run executed all job models or a single model.",
                max_length=20,
            ),
        ),
        migrations.AddIndex(
            model_name="taskrunhistory",
            index=models.Index(
                fields=["trigger"], name="job_schedul_trigger_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="taskrunhistory",
            index=models.Index(
                fields=["scope"], name="job_schedul_scope_idx"
            ),
        ),
    ]
