from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, AbstractUser
import os
from FeatureApp.storage import CleanFileNameStorage


class Users(AbstractUser):
    is_verified = models.BooleanField(default=False)
    admin_migrations = models.TextField(null=True)


class migrations(models.Model):
    Project_Version_Id = models.CharField(max_length=100, default=0)
    Migration_TypeId = models.CharField(max_length=100)
    Code = models.CharField(max_length=100, blank=True)
    Object_Type = models.CharField(max_length=100, blank=True)


class Feature(models.Model):
    choices = [
        ('Programlevel', 'programlevel'),
        ('Statementlevel', 'statementlevel'),
    ]
    Migration_TypeId = models.CharField(max_length=50)
    Feature_Id = models.BigAutoField(primary_key=True)
    Project_Version_Id = models.SmallIntegerField(default=0)
    Feature_Version_Id = models.SmallIntegerField(default=0)
    Object_Type = models.CharField(max_length=50)
    Feature_Name = models.CharField(max_length=100)
    Feature_version_approval_status = models.CharField(max_length=50, default='In Progress')
    Level = models.CharField(max_length=50, choices=choices, null=True, blank=True)
    Keywords = models.TextField(blank=True, null=True)
    Estimations = models.TextField(blank=True, null=True)
    Sequence = models.CharField(max_length=50)
    Source_FeatureDescription = models.TextField(blank=True, null=True)
    Source_Code = models.TextField(blank=True, null=True)
    Conversion_Code = models.TextField(blank=True, null=True)
    Target_FeatureDescription = models.TextField(blank=True, null=True)
    Target_Expected_Output = models.TextField(blank=True, null=True)
    Target_ActualCode = models.TextField(blank=True, null=True)
    Feature_Approval_Date = models.DateField(blank=True, null=True)

    def __int__(self):
        return self.Feature_Id


# def user_directory_path(instance, filename):
#     path_file = 'media/' + 'Project_version_' + str(instance.Feature_Id.Project_Version_Id) + '/' + instance.Feature_Id.Migration_TypeId + '/' + instance.Feature_Id.Object_Type + '/' + instance.Feature_Id.Feature_Name + '/' + 'Version_' + str(instance.Feature_Id.Feature_Version_Id) + '/' + instance.AttachmentType + '/' + filename
#     if os.path.exists(path_file):
#         os.remove(path_file)
#     for row in Attachments.objects.all().reverse():
#         if Attachments.objects.filter(filename=row.filename, AttachmentType=row.AttachmentType,
#                                       Feature_Id_id=row.Feature_Id_id).count() > 1:
#             row.delete()
#     return 'media/Project_version_{0}/{1}/{2}/{3}/Version_{4}/{5}/{6}'.format(instance.Feature_Id.Project_Version_Id,instance.Feature_Id.Migration_TypeId, instance.Feature_Id.Object_Type,
#                                               instance.Feature_Id.Feature_Name,instance.Feature_Id.Feature_Version_Id,
#                                               instance.AttachmentType, filename)

def user_directory_path(instance, filename):
    path_file = 'media/' + instance.Feature_Id.Migration_TypeId + '/' + instance.Feature_Id.Object_Type + '/' + instance.Feature_Id.Feature_Name + '/' + instance.AttachmentType + '/' + filename
    if os.path.exists(path_file):
        os.remove(path_file)
    for row in Attachments.objects.all().reverse():
        if Attachments.objects.filter(filename=row.filename, AttachmentType=row.AttachmentType,
                                      Feature_Id_id=row.Feature_Id_id).count() > 1:
            row.delete()
    return 'media/{0}/{1}/{2}/{3}/{4}'.format(instance.Feature_Id.Migration_TypeId, instance.Feature_Id.Object_Type,
                                              instance.Feature_Id.Feature_Name,
                                              instance.AttachmentType, filename)


class Attachments(models.Model):
    choices = [
        ('Sourcedescription', 'sourcedescription'),
        ('Targetdescription', 'targetdescription'),
        ('Conversion', 'conversion'),
        ('Sourcecode', 'sourcecode'),
        ('Actualtargetcode', 'actualtargetcode'),
        ('Expectedconversion', 'expectedconversion'),
    ]
    Project_Version_Id = models.SmallIntegerField(default=0)
    Feature_Version_Id = models.SmallIntegerField(default=0)
    Feature_Id = models.ForeignKey(Feature, on_delete=models.CASCADE, null=True)
    AttachmentType = models.CharField(max_length=50, blank=True, null=True, choices=choices)
    filename = models.CharField(max_length=100, blank=True, null=True)
    Attachment = models.FileField(upload_to=user_directory_path, blank=True, null=True, storage=CleanFileNameStorage(),max_length=500)

    def __int__(self):
        return self.Feature_Id.Feature_Id


class Approvals(models.Model):
    User_Email = models.CharField(max_length=100)
    Project_Version_Id = models.SmallIntegerField(default=0)
    Migration_TypeId = models.CharField(max_length=50, null=True)
    Object_Type = models.CharField(max_length=100)
    Feature_Name = models.CharField(max_length=100)
    Access_Type = models.CharField(max_length=100)
    Approval_Status = models.CharField(max_length=100)
    Approved_by = models.CharField(max_length=100, null=True, blank=True)
    Created_at = models.DateField(auto_now_add=True)
    Expiry_date = models.DateField(null=True, blank=True)


class Permissions(models.Model):
    User_Email = models.CharField(max_length=100)
    Project_Version_Id = models.SmallIntegerField(default=0)
    Migration_TypeId = models.CharField(max_length=100, null=True)
    Object_Type = models.CharField(max_length=100)
    Feature_Name = models.CharField(max_length=100)
    Access_Type = models.CharField(max_length=100)
    Approved_by = models.CharField(max_length=100)
    Created_at = models.DateField(auto_now_add=True)
    Expiry_date = models.DateField(null=True, blank=True)



