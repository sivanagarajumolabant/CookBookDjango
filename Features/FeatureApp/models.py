from django.db import models


# Create your models here.
class Feature(models.Model):
    choices = [
        ('Programlevel', 'programlevel'),
        ('Statementlevel', 'statementlevel'),
    ]
    Migration_TypeId = models.CharField(max_length=50)
    Level = models.CharField(max_length=50, choices=choices, null=True, blank=True)
    Version_Id = models.SmallIntegerField(default=0)
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
                       'Materialized views': 'Mate', 'Sequences': 'Sequ', 'Synonyms': 'Syno', 'Tabels': 'Tabe',
                       'Triggers': 'Trig', 'Types': 'Type', 'Views': 'view'}
        self.Feature_Name = object_dict[self.Object_Type] + '_' + self.Feature_Name
        self.Version_Id = self.Version_Id + 1
        self.Feature_Version = self.Feature_Version + 1
        super().save(*args, **kwargs)


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
    Attachment = models.FileField(upload_to='media/', blank=True, null=True)

    def __int__(self):
        return self.Feature_Id.id
