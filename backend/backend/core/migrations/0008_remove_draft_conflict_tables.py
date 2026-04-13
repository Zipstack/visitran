# Generated manually — drops draft/conflict tables and obsolete ModelVersion fields.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0007_nullable_model_data"),
    ]

    operations = [
        # Drop TransformationConflict first (has FK to UserDraft)
        migrations.DeleteModel(
            name="TransformationConflict",
        ),
        migrations.DeleteModel(
            name="UserDraft",
        ),
        # Remove obsolete ModelVersion fields
        migrations.RemoveField(
            model_name="modelversion",
            name="is_published",
        ),
        migrations.RemoveField(
            model_name="modelversion",
            name="change_summary",
        ),
        migrations.RemoveField(
            model_name="modelversion",
            name="extracted_model_name",
        ),
        migrations.RemoveField(
            model_name="modelversion",
            name="extracted_source_table",
        ),
        migrations.RemoveField(
            model_name="modelversion",
            name="extracted_transformation_count",
        ),
        migrations.RemoveField(
            model_name="modelversion",
            name="extracted_has_incremental_filter",
        ),
        # Remove stale index that referenced is_published
        migrations.RemoveIndex(
            model_name="modelversion",
            name="idx_mv_proj_published",
        ),
    ]
