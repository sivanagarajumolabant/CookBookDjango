# Generated by Django 3.2.11 on 2022-06-17 09:04

import FeatureApp.backend.custom_azure
import FeatureApp.models
import django.contrib.auth.models
import django.contrib.auth.validators
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('auth', '0012_alter_user_first_name_max_length'),
    ]

    operations = [
        migrations.CreateModel(
            name='Approvals',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('User_Email', models.CharField(max_length=100)),
                ('Migration_TypeId', models.CharField(max_length=50, null=True)),
                ('Object_Type', models.CharField(max_length=100)),
                ('Feature_Name', models.CharField(max_length=100)),
                ('Access_Type', models.CharField(max_length=100)),
                ('Approval_Status', models.CharField(max_length=100)),
                ('Approved_by', models.CharField(blank=True, max_length=100, null=True)),
                ('Created_at', models.DateField(auto_now_add=True)),
                ('Expiry_date', models.DateField(blank=True, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Deploy',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('Migration_TypeId', models.CharField(max_length=100, null=True)),
                ('Deploy_Start_Time', models.DateTimeField(blank=True, null=True)),
                ('Deploy_End_Time', models.DateTimeField(blank=True, null=True)),
                ('Deployment_Status', models.CharField(default='Deploy in Progress', max_length=50)),
            ],
        ),
        migrations.CreateModel(
            name='Feature',
            fields=[
                ('Migration_TypeId', models.CharField(max_length=50)),
                ('Feature_Id', models.BigAutoField(primary_key=True, serialize=False)),
                ('Project_Version_Id', models.SmallIntegerField(default=0)),
                ('Feature_Version_Id', models.SmallIntegerField(default=0)),
                ('Object_Type', models.CharField(max_length=50)),
                ('Feature_Name', models.CharField(max_length=100)),
                ('Feature_version_approval_status', models.CharField(default='In Progress', max_length=50)),
                ('Level', models.CharField(blank=True, choices=[('Programlevel', 'programlevel'), ('Statementlevel', 'statementlevel')], max_length=50, null=True)),
                ('Keywords', models.TextField(blank=True, null=True)),
                ('Estimations', models.TextField(blank=True, null=True)),
                ('Sequence', models.CharField(max_length=50)),
                ('Source_FeatureDescription', models.TextField(blank=True, null=True)),
                ('Source_Code', models.TextField(blank=True, null=True)),
                ('Conversion_Code', models.TextField(blank=True, null=True)),
                ('Target_FeatureDescription', models.TextField(blank=True, null=True)),
                ('Target_Expected_Output', models.TextField(blank=True, null=True)),
                ('Target_ActualCode', models.TextField(blank=True, null=True)),
                ('Feature_Approval_Date', models.DateField(blank=True, null=True)),
                ('Feature_Created_by', models.CharField(blank=True, max_length=100, null=True)),
                ('Feature_Created_at', models.DateField(auto_now_add=True)),
                ('Last_Modified_by', models.CharField(blank=True, max_length=100, null=True)),
                ('Last_Modified_at', models.DateField(blank=True, null=True)),
                ('Feature_Requested_By', models.CharField(blank=True, max_length=100, null=True)),
                ('Feature_Requested_Date', models.DateField(blank=True, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='migrations',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('Project_Version_Id', models.CharField(default=0, max_length=100)),
                ('Migration_TypeId', models.CharField(max_length=100)),
                ('Code', models.CharField(blank=True, max_length=100)),
                ('Object_Type', models.CharField(blank=True, max_length=100)),
                ('Project_Version_limit', models.CharField(blank=True, max_length=50)),
                ('Feature_Version_Limit', models.CharField(blank=True, max_length=50)),
            ],
        ),
        migrations.CreateModel(
            name='Permissions',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('User_Email', models.CharField(max_length=100)),
                ('Migration_TypeId', models.CharField(max_length=100, null=True)),
                ('Object_Type', models.CharField(max_length=100)),
                ('Feature_Name', models.CharField(max_length=100)),
                ('Access_Type', models.CharField(max_length=100)),
                ('Approved_by', models.CharField(max_length=100)),
                ('Created_at', models.DateField(auto_now_add=True)),
                ('Expiry_date', models.DateField(blank=True, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Users',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('password', models.CharField(max_length=128, verbose_name='password')),
                ('last_login', models.DateTimeField(blank=True, null=True, verbose_name='last login')),
                ('is_superuser', models.BooleanField(default=False, help_text='Designates that this user has all permissions without explicitly assigning them.', verbose_name='superuser status')),
                ('username', models.CharField(error_messages={'unique': 'A user with that username already exists.'}, help_text='Required. 150 characters or fewer. Letters, digits and @/./+/-/_ only.', max_length=150, unique=True, validators=[django.contrib.auth.validators.UnicodeUsernameValidator()], verbose_name='username')),
                ('first_name', models.CharField(blank=True, max_length=150, verbose_name='first name')),
                ('last_name', models.CharField(blank=True, max_length=150, verbose_name='last name')),
                ('email', models.EmailField(blank=True, max_length=254, verbose_name='email address')),
                ('is_staff', models.BooleanField(default=False, help_text='Designates whether the user can log into this admin site.', verbose_name='staff status')),
                ('is_active', models.BooleanField(default=True, help_text='Designates whether this user should be treated as active. Unselect this instead of deleting accounts.', verbose_name='active')),
                ('date_joined', models.DateTimeField(default=django.utils.timezone.now, verbose_name='date joined')),
                ('is_verified', models.BooleanField(default=False)),
                ('admin_migrations', models.TextField(null=True)),
                ('user_migrations', models.TextField(null=True)),
                ('is_user_admin', models.BooleanField(default=False)),
                ('user_registration_status', models.TextField(default='Awaiting for admin approval')),
                ('groups', models.ManyToManyField(blank=True, help_text='The groups this user belongs to. A user will get all permissions granted to each of their groups.', related_name='user_set', related_query_name='user', to='auth.Group', verbose_name='groups')),
                ('user_permissions', models.ManyToManyField(blank=True, help_text='Specific permissions for this user.', related_name='user_set', related_query_name='user', to='auth.Permission', verbose_name='user permissions')),
            ],
            options={
                'verbose_name': 'user',
                'verbose_name_plural': 'users',
                'abstract': False,
            },
            managers=[
                ('objects', django.contrib.auth.models.UserManager()),
            ],
        ),
        migrations.CreateModel(
            name='Attachments',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('Project_Version_Id', models.SmallIntegerField(default=0)),
                ('Feature_Version_Id', models.SmallIntegerField(default=0)),
                ('AttachmentType', models.CharField(blank=True, choices=[('Sourcedescription', 'sourcedescription'), ('Targetdescription', 'targetdescription'), ('Conversion', 'conversion'), ('Sourcecode', 'sourcecode'), ('Actualtargetcode', 'actualtargetcode'), ('Expectedconversion', 'expectedconversion')], max_length=50, null=True)),
                ('filename', models.CharField(blank=True, max_length=100, null=True)),
                ('Attachment', models.FileField(blank=True, max_length=500, null=True, storage=FeatureApp.backend.custom_azure.AzureMediaStorage, upload_to=FeatureApp.models.user_directory_path)),
                ('Feature_Id', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='FeatureApp.feature')),
            ],
        ),
    ]
