from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, AbstractUser
import os
from FeatureApp.storage import CleanFileNameStorage


class migrations(models.Model):
    Migration_TypeId = models.CharField(max_length=100)
    Code = models.CharField(max_length=100, blank=True)
    Object_Type = models.CharField(max_length=100, blank=True)


class Approvals(models.Model):
    # choices = [
    #     ('Approve', 'approve'),
    #     ('Deny', 'deny'),
    # ]
    User_Email = models.CharField(max_length=100)
    Migration_TypeId = models.CharField(max_length=50, null=True)
    Object_Type = models.CharField(max_length=100)
    Feature_Name = models.CharField(max_length=100)
    Approval_Status = models.CharField(max_length=100)
    Access_Type = models.CharField(max_length=100)
    Created_at = models.DateField(auto_now_add=True)
    Expiry_date = models.DateField(null=True)
    Approved_by = models.CharField(max_length=100, null=True, blank=True)


class Permissions(models.Model):
    User_Email = models.CharField(max_length=100)
    Migration_TypeId = models.CharField(max_length=100, null=True)
    Object_Type = models.CharField(max_length=100)
    Feature_Name = models.CharField(max_length=100)
    Access_Type = models.CharField(max_length=100)
    Approved_by = models.CharField(max_length=100)
    Created_at = models.DateField(auto_now_add=True)
    Expiry_date = models.DateField(null=True, blank=True)


class Users(AbstractUser):
    is_verified = models.BooleanField(default=False)
    admin_migrations = models.TextField(null=True)

# class Feature(models.Model):
#     choices = [
#         ('Programlevel', 'programlevel'),
#         ('Statementlevel', 'statementlevel'),
#     ]
#     Migration_TypeId = models.CharField(max_length=50)
#     Level = models.CharField(max_length=50, choices=choices, null=True, blank=True)
#     Version_Id = models.SmallIntegerField(default=0)
#     Keywords = models.TextField(blank=True, null=True)
#     Estimations = models.TextField(blank=True, null=True)
#     Feature_Version = models.SmallIntegerField(default=0)
#     Object_Type = models.CharField(max_length=50)
#     Feature_Id = models.BigAutoField(primary_key=True)
#     Feature_Name = models.CharField(max_length=100, unique=True)
#     Sequence = models.CharField(max_length=50)
#     Source_FeatureDescription = models.TextField(blank=True, null=True)
#     Source_Code = models.TextField(blank=True, null=True)
#     Conversion_Code = models.TextField(blank=True, null=True)
#     Target_FeatureDescription = models.TextField(blank=True, null=True)
#     Target_Expected_Output = models.TextField(blank=True, null=True)
#     Target_ActualCode = models.TextField(blank=True, null=True)
#
#     def __int__(self):
#         return self.Feature_Id
#
#     def save(self, *args, **kwargs):
#         # object_dict = {'PROCEDURE': 'Proc', 'FUNCTION': 'Func', 'PACKAGE': 'Pack', 'INDEX': 'Inde',
#         #                'MATERIALIZED VIEW': 'Mate', 'SEQUENCE': 'Sequ', 'SYNONYM': 'Syno', 'TABLE': 'Tabl',
#         #                'TRIGGER': 'Trig', 'TYPE': 'Type', 'VIEW': 'view','JOB':'jobs','PROGRAM':'prog',"SCHEDULE":'Sche'}
#         # self.Feature_Name = object_dict[self.Object_Type].upper() + '_' + self.Feature_Name
#
#         self.Feature_Name = self.Object_Type[0:4].upper() + '_' + self.Feature_Name
#         self.Version_Id = self.Version_Id + 1
#         self.Feature_Version = self.Feature_Version + 1
#         super().save(*args, **kwargs)
#
#     # def save(self, *args, **kwargs):
#     #     print("count===", len(self.Object_Type))
#     #     if len(self.Object_Type) == 3:
#     #         object_type_name = self.Object_Type[0:3]
#     #     elif len(self.Object_Type) > 3:
#     #         object_type_name = self.Object_Type[0:4]
#     #     self.Feature_Name = object_type_name.upper() + '_' + self.Feature_Name
#     #     self.Version_Id = self.Version_Id + 1
#     #     self.Feature_Version = self.Feature_Version + 1
#     #     super().save(*args, **kwargs)


class Feature(models.Model):
    choices = [
        ('Programlevel', 'programlevel'),
        ('Statementlevel', 'statementlevel'),
    ]
    Migration_TypeId = models.CharField(max_length=50)
    Feature_Id = models.BigAutoField(primary_key=True)
    Project_Version_Id = models.SmallIntegerField(default=0)
    #Feature_Version_Id = models.SmallIntegerField(unique=True, default=0)
    Feature_Version_Id = models.SmallIntegerField(default=0)
    Object_Type = models.CharField(max_length=50)
    Feature_Name = models.CharField(max_length=100)
    Feature_version_approval_status = models.CharField(max_length=50, default='Pending')
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

    def __int__(self):
        return self.Feature_Id

    # def save(self, *args, **kwargs):
    #     # print(self.Feature_Name,self.Feature_version_Id)
    #     # # self.Feature_Name = self.Object_Type[0:4].upper() + '_' + self.Feature_Name
    #     # self.Feature_version_Id = self.Feature_version_Id + 1
    #     # print("====",self.Feature_Name,self.Feature_version_Id)
    #     super().save(*args, **kwargs)



def user_directory_path(instance, filename):

    path_file = 'media/' + instance.Feature_Id.Migration_TypeId + '/' + instance.Feature_Id.Object_Type + '/' + instance.Feature_Id.Feature_Name + '/' + instance.AttachmentType + '/' + filename
    if os.path.exists(path_file):
        os.remove(path_file)
    for row in Attachments.objects.all().reverse():
        if Attachments.objects.filter(filename=row.filename, AttachmentType=row.AttachmentType,
                                      Feature_Id_id=row.Feature_Id_id).count() > 1:
            row.delete()
    return 'media/{0}/{1}/{2}/{3}/{4}'.format(instance.Feature_Id.Migration_TypeId, instance.Feature_Id.Object_Type, instance.Feature_Id.Feature_Name,
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
    Feature_Id = models.ForeignKey(Feature, on_delete=models.CASCADE, null=True)
    AttachmentType = models.CharField(max_length=50, blank=True, null=True, choices=choices)
    filename = models.CharField(max_length=100, blank=True, null=True)
    Attachment = models.FileField(upload_to=user_directory_path, blank=True, null=True, storage=CleanFileNameStorage())

    def __int__(self):
        return self.Feature_Id.Feature_Id

    # def delete(self, using=None, keep_parents=False):
    #     self.Attachment.delete()
    #     return super(Attachments, self).delete()
