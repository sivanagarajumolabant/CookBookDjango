# Generated by Django 3.2.11 on 2022-03-29 09:22

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('FeatureApp', '0001_initial'),
    ]

    operations = [
        migrations.RenameField(
            model_name='feature',
            old_name='Feature_Version',
            new_name='Feature_Version_Id',
        ),
        migrations.RenameField(
            model_name='feature',
            old_name='Version_Id',
            new_name='Project_Version_Id',
        ),
        migrations.AddField(
            model_name='feature',
            name='Feature_version_approval_status',
            field=models.CharField(default='Pending', max_length=50),
        ),
        migrations.AlterField(
            model_name='feature',
            name='Feature_Name',
            field=models.CharField(max_length=100),
        ),
    ]