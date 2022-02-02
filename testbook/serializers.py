from rest_framework import serializers
from .models import *
from django.contrib.auth.models import User
from rest_framework.validators import UniqueValidator
from django.contrib.auth.password_validation import validate_password
import collections

from rest_framework_simplejwt.serializers import TokenObtainPairSerializer

class FileSerializer(serializers.ModelSerializer):
    class Meta:
        model = Feature
        fields = '__all__'

class SequenceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Feature
        fields = ('Feature_Name',)

class MyTokenObtainPairSerializer(TokenObtainPairSerializer):

    @classmethod
    def get_token(cls, user):
        token = super(MyTokenObtainPairSerializer, cls).get_token(user)

        # Add custom claims
        token['username'] = user.username
        return token


class RegisterSerializer(serializers.ModelSerializer):
    email = serializers.EmailField(
        required=True,
        validators=[UniqueValidator(queryset=User.objects.all())]
    )

    password = serializers.CharField(write_only=True, required=True, validators=[validate_password])
    password2 = serializers.CharField(write_only=True, required=True)

    class Meta:
        model = User
        fields = ('username', 'password', 'password2', 'email', 'first_name', 'last_name')
        extra_kwargs = {
            'first_name': {'required': True},
            'last_name': {'required': True}
        }

    def validate(self, attrs):
        if attrs['password'] != attrs['password2']:
            raise serializers.ValidationError({"password": "Password fields didn't match."})

        return attrs

    def create(self, validated_data):
        user = User.objects.create(
            username=validated_data['username'],
            email=validated_data['email'],
            first_name=validated_data['first_name'],
            last_name=validated_data['last_name']
        )

        user.set_password(validated_data['password'])
        user.save()

        return user

# class featurelistSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Feature
#         fields = '__all__'

# class featurenameSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Feature
#         fields = ('Feature_Name',)

# class PostSerializer(serializers.ModelSerializer):
#     # features1 = Upload_file.objects.all()
#     # serializer1 = serializers.PrimaryKeyRelatedField()
#     # Upload = serializers.PrimaryKeyRelatedField(queryset=Upload_file.objects.all())
#     class Meta:
#         model = Feature
#         # fields = ('Upload_file',)
#         fields = '__all__'
#         depth = 1

class FeatureSerializer(serializers.ModelSerializer):
    Feature_Id = serializers.IntegerField(required=False)
    class Meta:
        model = Feature
        fields = ['Migration_TypeId', 'Object_Type',
                                         'Feature_Id', 'Feature_Name', 'Level', 'Sequence', 'Source_FeatureDescription',
                                         'Source_Code', 'Conversion_Code',
                                         'Target_FeatureDescription', 'Target_Expected_Output',
                                         'Target_ActualCode', 'Source_Attachment', 'Conversion_Attachment', 'Target_Attachment']

        # read_only_fields = ('Feature_Id',)


#
# class FeatureSerializer(serializers.ModelSerializer):
#     # feature = uploadSerializer()
#     # uploadss = serializers.SlugRelatedField(
#     #     slug_field="value", many=True, queryset=Upload_file.objects.all()
#     # )
#     class Meta:
#         model = Feature
#         fields = '__all__'

# class commonSerializer(serializers.Serializer):
class commonSerializer(serializers.ModelSerializer):
    # upload_files = uploadSerializer()
    # features = FeatureSerializer()
    class Meta:
        model = Feature
        # fields = '__all__'
        fields = ['Migration_TypeId', 'Object_Type',
                                         'Feature_Id', 'Feature_Name', 'Level', 'Sequence', 'Source_FeatureDescription',
                                         'Source_Code', 'Conversion_Code',
                                         'Target_FeatureDescription', 'Target_Expected_Output',
                                         'Target_ActualCode', 'Source_Attachment', 'Conversion_Attachment', 'Target_Attachment']
    def create(self, validated_data):
        # feature = validated_data.pop('feature')
        # upload = validated_data.pop('upload_files')
        # upload = validated_data['upload_files']
        # print('upload data is : ', upload)
        abc = Feature.objects.create(**validated_data)
        # print('abc data: ', abc)
        # for file in upload:
        # Upload_file.objects.create(Feature_Id=abc, **upload)
        # print('upload_file data is : ', Upload_file)
        return abc
        print('validated_data is : ', validated_data)
        # xyz = {**collections.OrderedDict(validated_data), **upload}
        # return xyz

    def update(self, instance, validated_data):
        print('validated_data is : ', validated_data)
        # upload = validated_data.pop('upload_files')
        # print('upload value', upload)
        # upl = (instance.upload_files).all()
        # print('upl values', upl)
        # upl = list(upl)
        instance.Migration_TypeId = validated_data.get('Migration_TypeId', instance.Migration_TypeId)
        instance.Object_Type = validated_data.get('Object_Type', instance.Object_Type)
        instance.Feature_Name = validated_data.get('Feature_Name', instance.Feature_Name)
        instance.Level = validated_data.get('Level', instance.Level)
        instance.Sequence = validated_data.get('Sequence', instance.Sequence)
        instance.Source_FeatureDescription = validated_data.get('Source_FeatureDescription', instance.Source_FeatureDescription)
        instance.Source_Code = validated_data.get('Source_Code', instance.Source_Code)
        # instance.Conversion_Description = validated_data.get('Conversion_Description', instance.Conversion_Description)
        instance.Conversion_Code = validated_data.get('Conversion_Code', instance.Conversion_Code)
        instance.Target_FeatureDescription = validated_data.get('Target_FeatureDescription', instance.Target_FeatureDescription)
        instance.Target_Expected_Output = validated_data.get('Target_Expected_Output', instance.Target_Expected_Output)
        instance.Target_ActualCode = validated_data.get('Target_ActualCode', instance.Target_ActualCode)
        instance.Source_Attachment = validated_data.get('Source_Attachment', instance.Source_Attachment)
        instance.Conversion_Attachment = validated_data.get('Conversion_Attachment', instance.Conversion_Attachment)
        instance.Target_Attachment = validated_data.get('Target_Attachment', instance.Target_Attachment)
        instance.save()

        # for upload_data in upload:

        # upl1 = upl.pop(0)
        # print('upl1 values', upl1)
        # upl1.Source_Attachment = upload.get('Source_Attachment', upl1.Source_Attachment)
        # upl1.Conversion_Attachment = upload.get('Conversion_Attachment', upl1.Conversion_Attachment)
        # upl1.Target_Attachment = upload.get('Target_Attachment', upl1.Target_Attachment)
        # upl1.save()
        # instance = upl.pop(0)
        # instance.Source_Attachment = validated_data.get('Source_Attachment', instance.Source_Attachment)
        # instance.Conversion_Attachment = validated_data.get('Conversion_Attachment', instance.Conversion_Attachment)
        # instance.Target_Attachment = validated_data.get('Target_Attachment', instance.Target_Attachment)
        # instance.save()
        return instance


#
# class PostSerializer1(serializers.ModelSerializer):
#     var1 = PostSerializer(many=True, read_only=True)
#     class Meta:
#         model = Feature
#         # fields = ('var1', 'Source_Attachment', 'Conversion_Attachment', 'Target_Attachment')
#         fields = '__all__'

class migrationlevelfeatures(serializers.ModelSerializer):
    class Meta:
        model = Feature
        fields = ('Feature_Id','Feature_Name')
#
# class migrationtype(serializers.ModelSerializer):
#     class Meta:
#         model = Feature
#         fields = ('Migration_TypeId',)

# class migrationlevelfeatures1(serializers.ModelSerializer):
#     class Meta:
#         model = Feature
#         fields = ('Feature_Id','Object_Type', 'Feature_Name')

# class upload_fileSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Upload_file
#         fields = '__all__'


# class FSerializer(serializers.ModelSerializer):
#     """
#     Serializer to Add ModelA together with ModelB
#     """
#
#     class ModelBTempSerializer(serializers.ModelSerializer):
#         class Meta:
#             model = Upload_file
#             # 'model_a_field' is a FK which will be assigned after creation of 'ModelA' model entry
#             # First entry of ModelB will have (default) fieldB3 value as True, rest as False
#             # 'field4' will be derived from its counterpart from the 'Product' model attribute
#             # exclude = ['model_a_field', 'fieldB3', 'field4']
#
#     model_b = ModelBTempSerializer()
#
#     class Meta:
#         model = Feature
#         fields = '__all__'
#
#     def create(self, validated_data):
#         model_b_data = validated_data.pop('model_b')
#         Feature_Id = Feature.objects.create(**validated_data)
#         Upload_file.objects.create(Feature_Id=Feature_Id
#                               # fieldB3=True,
#                               # field4=model_a_instance.field4,
#                               **model_b_data)
#         return Feature_Id