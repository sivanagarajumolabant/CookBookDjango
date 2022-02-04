from rest_framework import serializers
from .models import *
from django.contrib.auth.models import User


class FeatureSerializer(serializers.ModelSerializer):
    class Meta:
        model = Feature
        fields = "__all__"


class AttachementSerializer(serializers.ModelSerializer):
    class Meta:
        model = Attachments
        fields = "__all__"


class SequenceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Feature
        fields = ('Feature_Name',)


class migrationlevelfeatures(serializers.ModelSerializer):
    class Meta:
        model = Feature
        fields = ('Feature_Id', 'Feature_Name',)
