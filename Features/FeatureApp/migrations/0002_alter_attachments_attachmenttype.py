# Generated by Django 3.2.10 on 2022-02-04 05:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('FeatureApp', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='attachments',
            name='AttachmentType',
            field=models.CharField(blank=True, choices=[('Sourcedescription', 'sourcedescription'), ('Targetdescription', 'targetdescription'), ('Conversion', 'conversion')], max_length=50, null=True),
        ),
    ]
