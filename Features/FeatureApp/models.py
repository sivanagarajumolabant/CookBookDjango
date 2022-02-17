from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, AbstractUser
import json


class Users(AbstractUser):
    # str1 = [{"Label": "Procedures", "subMenu": []}, {"Label": "Functions", "subMenu": []},
    #         {"Label": "Packages", "subMenu": []}, {"Label": "Indexes", "subMenu": []},
    #         {"Label": "Materialized views", "subMenu": []}]
    # obj = json.dumps(str1)
    is_verified = models.BooleanField(default=False)
    # can_view = models.TextField(default=obj)


# Create your models here.
class Feature(models.Model):
    choices = [
        ('Programlevel', 'programlevel'),
        ('Statementlevel', 'statementlevel'),
    ]

    Migration_TypeId = models.CharField(max_length=50)
    Level = models.CharField(max_length=50, choices=choices, null=True, blank=True)
    Version_Id = models.SmallIntegerField(default=0)
    Keywords = models.TextField(blank=True, null=True)
    Estimations = models.TextField(blank=True, null=True)
    Feature_Version = models.SmallIntegerField(default=0)
    Object_Type = models.CharField(max_length=50)
    Feature_Id = models.BigAutoField(primary_key=True)
    Feature_Name = models.CharField(max_length=100, unique=True)
    Sequence = models.CharField(max_length=50)
    Source_FeatureDescription = models.TextField(blank=True, null=True)
    Source_Code = models.TextField(blank=True, null=True)
    Conversion_Code = models.TextField(blank=True, null=True)
    Target_FeatureDescription = models.TextField(blank=True, null=True)
    Target_Expected_Output = models.TextField(blank=True, null=True)
    Target_ActualCode = models.TextField(blank=True, null=True)

    def __int__(self):
        return self.Feature_Id

    def save(self, *args, **kwargs):
        # if self.Object_Type == 'Procedure':
        object_dict = {'Procedure': 'Proc', 'Function': 'Func', 'Package': 'Pack', 'Index': 'Inde',
                       'Materialized view': 'Mate', 'Sequence': 'Sequ', 'Synonym': 'Syno', 'Tabel': 'Tabe',
                       'Trigger': 'Trig', 'Type': 'Type', 'View': 'view'}
        self.Feature_Name = object_dict[self.Object_Type] + '_' + self.Feature_Name
        self.Version_Id = self.Version_Id + 1
        self.Feature_Version = self.Feature_Version + 1
        super().save(*args, **kwargs)


def user_directory_path(instance, filename):
    print("instance ", instance)
    o2p = ''
    if instance.Feature_Id.Migration_TypeId == '1':
        o2p = 'Oracle To Postgres'
        # print(o2p)
    elif instance.Feature_Id.Migration_TypeId == '2':
        o2p = 'Oracle TO SQLServer'
        # print(o2p)
    elif instance.Feature_Id.Migration_TypeId == '3':
        o2p = 'Oracle To MYSQL'
        # print(o2p)
    # file will be uploaded to MEDIA_ROOT/user_<id>/<filename>
    return 'media/{0}/{1}/{2}/{3}/{4}'.format(o2p, instance.Feature_Id.Object_Type, instance.Feature_Id.Feature_Name,
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
    Attachment = models.FileField(upload_to=user_directory_path, blank=True, null=True)

    def __int__(self):
        return self.Feature_Id.Feature_Id
