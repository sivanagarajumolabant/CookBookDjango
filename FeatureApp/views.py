import imp
from django.shortcuts import render
from rest_framework import viewsets, status
from .serializers import *
from datetime import *
from config.config import frontend_url
from .models import *
from rest_framework.decorators import api_view
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
import mimetypes
import json
import os
from django.http import HttpResponse
# from importlib import import_module
from import_file import import_file
import re
import sys
from django.core import mail
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from Features.settings import EMAIL_HOST_USER
# from emailcontent import email_verification_data
from django.contrib.auth.decorators import permission_required
from Features.settings import BASE_DIR, MEDIA_ROOT

import jwt
from django.urls import reverse
from rest_framework import generics
from django.conf import settings
from django.contrib.sites.shortcuts import get_current_site
from rest_framework_simplejwt.tokens import RefreshToken
from .utils import Util
from rest_framework.decorators import permission_classes
from rest_framework.permissions import IsAuthenticated


# Create your views here.
@api_view(['POST'])
# @permission_classes([IsAuthenticated])
# @permission_required('FeatureApp.add_feature', raise_exception=True)
def featurecreate(request):
    serializer = FeatureSerializer(data=request.data)
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET'])
# @permission_classes([IsAuthenticated])
# @permission_required('FeatureApp.view_feature', raise_exception=True)
def featurelist(request):
    features = Feature.objects.all()
    serializer = FeatureSerializer(features, many=True)
    return Response(serializer.data)


@api_view(['POST'])
def featuredropdownlist(request):
    body_unicode = request.body.decode('utf-8')
    body_data = json.loads(body_unicode)
    migtype = body_data['Migration_TypeId']
    obj_type = body_data['Object_Type']
    # print("mig ",migtype)
    # print("type",obj_type)
    features = Feature.objects.filter(
        Object_Type=obj_type, Migration_TypeId=migtype)
    serializer = FeaturedropdownSerializer(features, many=True)
    return Response(serializer.data)


# @api_view(['GET'])
# def featuredetail(request, pk):
#     features = Feature.objects.get(Feature_Id=pk)
#     serializer = FeatureSerializer(features, many=False)
#     return Response(serializer.data)

# @api_view(['POST'])
# def featuredetail(request, pk):
#     email = request.data['User_Email']
#     user = Users.objects.get(email=email)
#     if user.is_superuser:
#         EDIT = 1
#     else:
#         feature_name = Feature.objects.filter(Feature_Id=pk)
#         feature_name = list(feature_name.values('Feature_Name'))
#         feature_name = feature_name[0]
#         feature_name = feature_name['Feature_Name']
#         perm_data = Permissions.objects.filter(User_Email=email,
#                                                Access_Type='Edit')
#         features_objects = list(perm_data.values('Feature_Name'))
#         list_of_feature_values = [
#             value for elem in features_objects for value in elem.values()]
#         if feature_name not in list_of_feature_values:
#             EDIT = 0
#         else:
#             EDIT = 1
#     features = Feature.objects.get(Feature_Id=pk)
#     serializer = FeatureSerializer(features, many=False)
#     response = {'edit': EDIT, 'serializer': serializer.data}
#     return Response(response)


# @api_view(['POST'])
# def featuredetail(request, pk):
#     email = request.data['User_Email']
#     user = Users.objects.filter(email=email)
#
#     user_values = list(user.values())
#     user_is_superuser = user_values[0]['is_superuser']
#     user_admin_list = user_values[0]['admin_migrations']
#     if user_admin_list != None:
#         user_admin_list = user_admin_list.split('.')
#         user_admin_list = [x for x in user_admin_list if x]
#     else :
#         user_admin_list = []
#
#     feature = Feature.objects.filter(Feature_Id=pk)
#     mig_type = feature.values()[0]['Migration_TypeId']
#
#     if user_is_superuser == True or mig_type in user_admin_list :
#         EDIT = 1
#         # Flag = 3
#     else:
#         feature_name = Feature.objects.filter(Feature_Id=pk)
#         feature_name = list(feature_name.values('Feature_Name'))
#         feature_name = feature_name[0]
#         feature_name = feature_name['Feature_Name']
#         perm_data = Permissions.objects.filter(User_Email=email,
#                                                Access_Type='Edit')
#         perm_data_view = Permissions.objects.filter(User_Email=email,
#                                                Access_Type='View')
#         features_objects = list(perm_data.values('Feature_Name'))
#         list_of_feature_values = [
#             value for elem in features_objects for value in elem.values()]
#
#         features_objects_view = list(perm_data_view.values('Feature_Name'))
#         list_of_feature_values_view = [
#             value for elem in features_objects_view for value in elem.values()]
#
#         if feature_name not in list_of_feature_values:
#             EDIT = 0
#             # if feature_name in list_of_feature_values_view:
#             #     Flag =2
#             # else:
#             #     Flag = 1
#         else:
#             EDIT = 1
#             # Flag = 3
#     features = Feature.objects.get(Feature_Id=pk)
#     serializer = FeatureSerializer(features, many=False)
#     response = {'edit': EDIT, 'serializer': serializer.data}#,'flag': Flag}
#     return Response(response)


# @api_view(['POST'])
# def featuredetail(request, pk):
#     email = request.data['User_Email']
#     user = Users.objects.filter(email=email)
#
#     user_values = list(user.values())
#     user_is_superuser = user_values[0]['is_superuser']
#     user_admin_list = user_values[0]['admin_migrations']
#     if user_admin_list != None:
#         user_admin_list = user_admin_list.split('.')
#         user_admin_list = [x for x in user_admin_list if x]
#     else :
#         user_admin_list = []
#
#     feature = Feature.objects.filter(Feature_Id=pk)
#     mig_type = feature.values()[0]['Migration_TypeId']
#     obj_type = feature.values()[0]['Object_Type']
#     feature_name = feature.values()[0]['Feature_Name']
#
#     EDIT = 0
#
#     if user_is_superuser == True or mig_type in user_admin_list :
#         EDIT = 1
#     else:
#         perm_data1 = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Object_Type=obj_type,Feature_Name=feature_name)
#         if perm_data1:
#             data1_access = perm_data1.values()[0]['Access_Type']
#             if data1_access in ('Edit', 'ALL'):
#                 EDIT = 1
#
#         perm_data2 = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Object_Type=obj_type,Feature_Name='ALL')
#         if perm_data2:
#             data2_access = perm_data2.values()[0]['Access_Type']
#             if data2_access in ('Edit', 'ALL'):
#                 EDIT = 1
#
#         perm_data3 = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Object_Type='ALL')
#         if perm_data3:
#             data3_access = perm_data3.values()[0]['Access_Type']
#             if data3_access in ('Edit', 'ALL'):
#                 EDIT = 1
#
#     features = Feature.objects.get(Feature_Id=pk)
#     serializer = FeatureSerializer(features, many=False)
#     response = {'edit': EDIT, 'serializer': serializer.data}
#     # print(response)
#     return Response(response)


# @api_view(['POST'])
# def featuredetail(request, pk):
#     email = request.data['User_Email']
#     user = Users.objects.filter(email=email)
#
#     user_values = list(user.values())
#     user_is_superuser = user_values[0]['is_superuser']
#     user_admin_list = user_values[0]['admin_migrations']
#     if user_admin_list != None:
#         user_admin_list = user_admin_list.split(',')
#         user_admin_list = [x for x in user_admin_list if x]
#     else :
#         user_admin_list = []
#
#     feature = Feature.objects.filter(Feature_Id=pk)
#     mig_type = feature.values()[0]['Migration_TypeId']
#     obj_type = feature.values()[0]['Object_Type']
#     feature_name = feature.values()[0]['Feature_Name']
#
#     EDIT = 0
#     if user_is_superuser == True or mig_type in user_admin_list :
#         EDIT = 1
#     else:
#         perm_data1 = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Object_Type=obj_type,Feature_Name=feature_name)
#         if perm_data1:
#             data1_access = perm_data1.values()[0]['Access_Type']
#             if data1_access in ('Edit', 'ALL'):
#                 EDIT = 1
#         perm_data2 = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Object_Type=obj_type,Feature_Name='ALL')
#         if perm_data2:
#             data2_access = perm_data2.values()[0]['Access_Type']
#             if data2_access in ('Edit', 'ALL'):
#                 EDIT = 1
#         perm_data3 = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Object_Type='ALL')
#         if perm_data3:
#             data3_access = perm_data3.values()[0]['Access_Type']
#             if data3_access in ('Edit', 'ALL'):
#                 EDIT = 1
#         if not perm_data1 and perm_data2 and perm_data3:
#             EDIT = 0
#     features = Feature.objects.get(Feature_Id=pk)
#     serializer = FeatureSerializer(features, many=False)
#     response = {'edit': EDIT, 'serializer': serializer.data}
#     return Response(response)


@api_view(['POST'])
def featuredetail(request, pk):
    email = request.data['User_Email']
    user = Users.objects.filter(email=email)

    user_values = list(user.values())
    user_is_superuser = user_values[0]['is_superuser']
    admin_access = user_values[0]['admin_migrations']
    if admin_access != '':
        admin_access = admin_access.replace("\'", "\"")
        admin_access_dict = json.loads(admin_access)
    else:
        admin_access_dict = {}

    feature = Feature.objects.filter(Feature_Id=pk)
    mig_type = feature.values()[0]['Migration_TypeId']
    obj_type = feature.values()[0]['Object_Type']
    feature_name = feature.values()[0]['Feature_Name']

    EDIT = 0
    if user_is_superuser == True :
        EDIT = 1
    elif mig_type in admin_access_dict.keys():
        if obj_type in admin_access_dict[mig_type]:
            EDIT = 1
    else:
        perm_data1 = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Object_Type=obj_type,Feature_Name=feature_name)
        if perm_data1:
            data1_access = perm_data1.values()[0]['Access_Type']
            if data1_access in ('Edit', 'ALL'):
                EDIT = 1
        perm_data2 = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Object_Type=obj_type,Feature_Name='ALL')
        if perm_data2:
            data2_access = perm_data2.values()[0]['Access_Type']
            if data2_access in ('Edit', 'ALL'):
                EDIT = 1
        perm_data3 = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Object_Type='ALL')
        if perm_data3:
            data3_access = perm_data3.values()[0]['Access_Type']
            if data3_access in ('Edit', 'ALL'):
                EDIT = 1
        if not perm_data1 and perm_data2 and perm_data3:
            EDIT = 0
    features = Feature.objects.get(Feature_Id=pk)
    serializer = FeatureSerializer(features, many=False)
    response = {'edit': EDIT, 'serializer': serializer.data}
    return Response(response)

# @api_view(['GET','POST'])
# def featuredetail(request, pk):
#     email = request.data['User_Email']
#     user = Users.objects.get(email=email)
#     if user.is_superuser:
#         EDIT = 1
#     else:
#         feature_name = Feature.objects.filter(Feature_Id=pk)
#         feature_name = list(feature_name.values('Feature_Name'))
#         feature_name = feature_name[0]
#         feature_name = feature_name['Feature_Name']
#         perm_data = Permissions.objects.filter(User_Email=email,
#                                                Access_Type='edit')
#         features_objects = list(perm_data.values('Feature_Name'))
#         list_of_feature_values = [value for elem in features_objects for value in elem.values()]
#         if feature_name not in list_of_feature_values:
#             EDIT = 0
#         else:
#             EDIT = 1
#     features = Feature.objects.get(Feature_Id=pk)
#     serializer = FeatureSerializer(features, many=False)
#     response = {'edit': EDIT, 'serializer': serializer.data}
#     return Response(response)


# @api_view(['GET','POST'])
# def feature_catalog_access_check(request):
#     user_email = request.data['User_Email']
#     mig_type = request.data['Migration_Type']
#     obj_type = request.data['Object_Type']
#     feature_name = request.data['Feature_Name']
#     # print(user_email,mig_type,obj_type,feature_name)
#
#     perm_data = Permissions.objects.filter(User_Email=user_email, Migration_TypeId=mig_type, Object_Type=obj_type,
#                                            Feature_Name=feature_name)
#     if perm_data:
#         access_data = perm_data.values('Access_Type')[0]['Access_Type']
#
#         if obj_type == 'ALL':
#             if access_data == 'ALL' or access_data == 'Edit':
#                 flag = 3
#             elif access_data == 'View':
#                 flag = 2
#             else:
#                 flag = 1
#         elif feature_name == 'ALL':
#             if access_data == 'ALL' or access_data == 'Edit':
#                 flag = 3
#             elif access_data == 'View':
#                 flag = 2
#             else:
#                 flag = 1
#         else:
#             if access_data == 'ALL' or access_data == 'Edit':
#                 flag = 3
#             elif access_data == 'View':
#                 flag = 2
#             else:
#                 flag = 1
#     else:
#         flag = 1
#     # serializer = PermissionSerializer(perm_data, many=True)
#     if feature_name!='ALL':
#         features = Feature.objects.get(Migration_TypeId=mig_type, Object_Type=obj_type,
#                                         Feature_Name=feature_name)
#         serializer = FeatureSerializer(features, many=False)
#         response = {'serializer': serializer.data, 'flag': flag}
#     else:
#         response = {'serializer': 'No Data', 'flag': flag}
#     return Response(response)


# @api_view(['GET','POST'])
# def feature_catalog_access_check(request):
#     user_email = request.data['User_Email']
#     mig_type = request.data['Migration_Type']
#     obj_type = request.data['Object_Type']
#     feature_name = request.data['Feature_Name']
#
#     user = Users.objects.filter(email=user_email)
#
#     user_values = list(user.values())
#     user_is_superuser = user_values[0]['is_superuser']
#     user_admin_list = user_values[0]['admin_migrations']
#     if user_admin_list != None:
#         user_admin_list = user_admin_list.split('.')
#         user_admin_list = [x for x in user_admin_list if x]
#     else:
#         user_admin_list = []
#
#     flag = 1
#
#     if user_is_superuser == True or mig_type in user_admin_list:
#         flag = 3
#     else:
#         if obj_type != 'ALL' and feature_name != 'ALL':
#             perm_data1 = Permissions.objects.filter(User_Email=user_email, Migration_TypeId=mig_type, Object_Type=obj_type,
#                                                     Feature_Name=feature_name)
#             if perm_data1:
#                 data1_access = perm_data1.values()[0]['Access_Type']
#                 if data1_access in ('Edit', 'ALL'):
#                     flag = 3
#                 elif data1_access == 'View':
#                     flag = 2
#                 else:
#                     flag = 1
#         elif feature_name == 'ALL' and obj_type != 'ALL':
#             perm_data2 = Permissions.objects.filter(User_Email=user_email, Migration_TypeId=mig_type, Object_Type=obj_type,
#                                                     Feature_Name='ALL')
#             if perm_data2:
#                 data1_access = perm_data2.values()[0]['Access_Type']
#                 if data1_access in ('Edit', 'ALL'):
#                     flag = 3
#                 elif data1_access == 'View':
#                     flag = 2
#                 else:
#                     flag = 1
#         elif obj_type == 'ALL':
#             perm_data3 = Permissions.objects.filter(User_Email=user_email, Migration_TypeId=mig_type, Object_Type='ALL')
#             if perm_data3:
#                 data3_access = perm_data3.values()[0]['Access_Type']
#                 if data3_access in ('Edit', 'ALL'):
#                     flag = 3
#                 elif data3_access == 'View':
#                     flag = 2
#                 else:
#                     flag = 1
#
#     if feature_name!='ALL' and obj_type != 'ALL':
#         features = Feature.objects.get(Migration_TypeId=mig_type, Object_Type=obj_type,
#                                         Feature_Name=feature_name)
#         serializer = FeatureSerializer(features, many=False)
#         response = {'serializer': serializer.data, 'flag': flag}
#     else:
#         response = {'serializer': 'No Data', 'flag': flag}
#     return Response(response)


# @api_view(['GET','POST'])
# def feature_catalog_access_check(request):
#     user_email = request.data['User_Email']
#     mig_type = request.data['Migration_Type']
#     obj_type = request.data['Object_Type']
#     feature_name = request.data['Feature_Name']
#
#     user = Users.objects.filter(email=user_email)
#
#     user_values = list(user.values())
#     user_is_superuser = user_values[0]['is_superuser']
#     user_admin_list = user_values[0]['admin_migrations']
#     if user_admin_list != None:
#         user_admin_list = user_admin_list.split(',')
#         user_admin_list = [x for x in user_admin_list if x]
#     else:
#         user_admin_list = []
#     flag = 1
#     if user_is_superuser == True or mig_type in user_admin_list:
#         flag = 3
#     elif obj_type != ''  and feature_name != '':
#         perm_data1 = Permissions.objects.filter(User_Email=user_email, Migration_TypeId=mig_type, Object_Type=obj_type,
#                                                 Feature_Name=feature_name)
#         if perm_data1:
#             data1_access = perm_data1.values()[0]['Access_Type']
#             if data1_access in ('Edit', 'ALL'):
#                 flag = 3
#             elif data1_access == 'View':
#                 flag = 2
#             else:
#                 flag = 1
#         perm_data2 = Permissions.objects.filter(User_Email=user_email, Migration_TypeId=mig_type, Object_Type=obj_type,
#                                                 Feature_Name='ALL')
#         if perm_data2:
#             data2_access = perm_data2.values()[0]['Access_Type']
#             if data2_access in ('Edit', 'ALL'):
#                 flag = 3
#             elif data2_access == 'View':
#                 flag = 2
#             else:
#                 flag = 1
#         perm_data3 = Permissions.objects.filter(User_Email=user_email, Migration_TypeId=mig_type, Object_Type='ALL')
#         if perm_data3:
#             data3_access = perm_data3.values()[0]['Access_Type']
#             if data3_access in ('Edit', 'ALL'):
#                 flag = 3
#             elif data3_access == 'View':
#                 flag = 2
#             else:
#                 flag = 1
#         if not perm_data1 and perm_data2 and perm_data3:
#             flag = 1
#     if feature_name!='ALL' and obj_type != 'ALL':
#         features = Feature.objects.get(Migration_TypeId=mig_type, Object_Type=obj_type,
#                                         Feature_Name=feature_name)
#         serializer = FeatureSerializer(features, many=False)
#         response = {'serializer': serializer.data, 'flag': flag}
#     else:
#         response = {'serializer': 'No Data', 'flag': flag}
#     return Response(response)


@api_view(['GET','POST'])
def feature_catalog_access_check(request):
    user_email = request.data['User_Email']
    mig_type = request.data['Migration_Type']
    obj_type = request.data['Object_Type']
    feature_name = request.data['Feature_Name']

    user = Users.objects.filter(email=user_email)

    user_values = list(user.values())
    user_is_superuser = user_values[0]['is_superuser']
    admin_access = user_values[0]['admin_migrations']
    if admin_access != '':
        admin_access = admin_access.replace("\'", "\"")
        admin_access_dict = json.loads(admin_access)
    else:
        admin_access_dict = {}

    flag = 1
    if user_is_superuser == True :
        flag = 3
    elif mig_type in admin_access_dict.keys():
        if obj_type in admin_access_dict[mig_type]:
            flag = 3
    elif obj_type != ''  and feature_name != '':
        perm_data1 = Permissions.objects.filter(User_Email=user_email, Migration_TypeId=mig_type, Object_Type=obj_type,
                                                Feature_Name=feature_name)
        if perm_data1:
            data1_access = perm_data1.values()[0]['Access_Type']
            if data1_access in ('Edit', 'ALL'):
                flag = 3
            elif data1_access == 'View':
                flag = 2
            else:
                flag = 1
        perm_data2 = Permissions.objects.filter(User_Email=user_email, Migration_TypeId=mig_type, Object_Type=obj_type,
                                                Feature_Name='ALL')
        if perm_data2:
            data2_access = perm_data2.values()[0]['Access_Type']
            if data2_access in ('Edit', 'ALL'):
                flag = 3
            elif data2_access == 'View':
                flag = 2
            else:
                flag = 1
        perm_data3 = Permissions.objects.filter(User_Email=user_email, Migration_TypeId=mig_type, Object_Type='ALL')
        if perm_data3:
            data3_access = perm_data3.values()[0]['Access_Type']
            if data3_access in ('Edit', 'ALL'):
                flag = 3
            elif data3_access == 'View':
                flag = 2
            else:
                flag = 1
        if not perm_data1 and perm_data2 and perm_data3:
            flag = 1
    if feature_name!='ALL' and obj_type != 'ALL':
        features = Feature.objects.get(Migration_TypeId=mig_type, Object_Type=obj_type,
                                        Feature_Name=feature_name)
        serializer = FeatureSerializer(features, many=False)
        response = {'serializer': serializer.data, 'flag': flag}
    else:
        response = {'serializer': 'No Data', 'flag': flag}
    return Response(response)

@api_view(['PUT'])
def featureupdate(request, pk):
    feature = Feature.objects.get(Feature_Id=pk)
    serializer = FeatureSerializer(instance=feature, data=request.data)
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data, status=status.HTTP_200_OK)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET'])
def att_list(request):
    att = Attachments.objects.all()
    serializer = AttachementSerializer(att, many=True)
    return Response(serializer.data)


@api_view(['GET'])
def Sourcedescription(request, id):
    features = Attachments.objects.filter(
        Feature_Id=id, AttachmentType='Sourcedescription')
    serializer = AttachementSerializer(features, many=True)
    return Response(serializer.data)


@api_view(['GET'])
def Targetdescription(request, id):
    features = Attachments.objects.filter(
        Feature_Id=id, AttachmentType='Targetdescription')
    serializer = AttachementSerializer(features, many=True)
    return Response(serializer.data)


@api_view(['GET'])
def Conversion(request, id):
    features = Attachments.objects.filter(
        Feature_Id=id, AttachmentType='Conversion')
    serializer = AttachementSerializer(features, many=True)
    return Response(serializer.data)


# @api_view(['GET'])
# def Sourcecode(request, id):
#     features = Attachments.objects.filter(Feature_Id=id, AttachmentType='Sourcecode')
#     serializer = AttachementSerializer(features, many=True)
#     return Response(serializer.data)
#
# @api_view(['GET'])
# def Actualtargetcode(request, id):
#     features = Attachments.objects.filter(Feature_Id=id, AttachmentType='Actualtargetcode')
#     serializer = AttachementSerializer(features, many=True)
#     return Response(serializer.data)
#
# @api_view(['GET'])
# def Expectedconversion(request, id):
#     features = Attachments.objects.filter(Feature_Id=id, AttachmentType='Expectedconversion')
#     serializer = AttachementSerializer(features, many=True)
#     return Response(serializer.data)


@api_view(['POST'])
def attachment_delete(request):
    file_name = request.data['file_name']
    migration_typeid = request.data['migration_typeid']
    object_type = request.data['object_type']
    AttachmentType = request.data['AttachmentType']
    id = request.data['id']
    featurename = request.data['fname']
    attachment = Attachments.objects.get(id=id)
    attachment.delete()
    fl_path = MEDIA_ROOT + '/media' + '/' + migration_typeid + '/' + \
        object_type + '/' + featurename + '/' + AttachmentType + '/'
    filename = fl_path + file_name
    os.remove(filename)
    return Response('Deleted')


@api_view(['POST'])
def Attcahmentupdate(request, pk):
    feature = Feature.objects.get(Feature_Id=pk)
    AttachmentType = request.data['AttachmentType']
    Attachment = request.FILES['Attachment']
    filename = request.data['filename']
    dictionary = {"Feature_Id": feature, 'AttachmentType': AttachmentType, "filename": filename,
                  "Attachment": Attachment}
    attachements = AttachementSerializer(data=dictionary)
    if attachements.is_valid():
        attachements.save()
        for row in Attachments.objects.all().reverse():
            if Attachments.objects.filter(filename=row.filename, AttachmentType=row.AttachmentType,
                                          Feature_Id_id=row.Feature_Id_id).count() > 1:
                row.delete()
        return Response(attachements.data, status=status.HTTP_200_OK)
    return Response(attachements.errors, status=status.HTTP_400_BAD_REQUEST)


# @api_view(['DELETE'])
# def featuredelete(request, pk):
#     features = Feature.objects.get(Feature_Id=pk)
#     features.delete()
#     return Response('Deleted')

@api_view(['DELETE'])
def featuredelete(request, pk):
    features = Feature.objects.filter(Feature_Id=pk)
    feature_name = features.values('Feature_Name')[0]['Feature_Name']
    appr_data = Approvals.objects.filter(Feature_Name=feature_name)
    perm_data = Permissions.objects.filter(Feature_Name=feature_name)
    appr_data.delete()
    perm_data.delete()
    features.delete()
    return Response('Deleted')


@api_view(['POST'])
# def sequence(request, Object_Type, Migration_TypeId):
def predessors(request):
    body_unicode = request.body.decode('utf-8')
    body_data = json.loads(body_unicode)
    Object_Type = body_data['Object_Type']
    Migration_TypeId = body_data['Migration_TypeId']
    features = Feature.objects.filter(
        Object_Type=Object_Type, Migration_TypeId=Migration_TypeId)
    serializer = SequenceSerializer(features, many=True)
    return Response(serializer.data, status=status.HTTP_200_OK)


@api_view(['POST'])
def download_attachment(request):
    body_unicode = request.body.decode('utf-8')
    body_data = json.loads(body_unicode)
    file_name = body_data['file_name']
    migration_typeid = body_data['migration_typeid']
    attach_type = body_data['AttachmentType']
    object_type = body_data['object_type']
    featurename = body_data['fname']
    fid = body_data['feature_id']
    filter_files = Attachments.objects.filter(
        Feature_Id=fid, AttachmentType=attach_type, filename=file_name)
    filter_values = list(filter_files.values_list())
    file_path = filter_values[0]
    fl = open(file_path[4], 'rb')
    mime_type, _ = mimetypes.guess_type(file_path[4])
    response = HttpResponse(fl, content_type=mime_type)
    response['Content-Disposition'] = "attachment; filename=%s" % file_name
    return response


@api_view(['POST'])
def conversion(request):
    # try:
    body_unicode = request.body.decode('utf-8')
    body_data = json.loads(body_unicode)
    feature_name = body_data['featurename']
    python_code = body_data['convcode']
    source_code = body_data['sourcecode']
    migration_typeid = body_data['migration_typeid']
    object_type = body_data['object_type']
    path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    module_folder_name = "Modules"
    module_path = path + '/' + module_folder_name + \
        '/' + migration_typeid + '/' + object_type
    sys.path.append(module_path)
    if not os.path.exists(module_path):
        os.makedirs(module_path)
    python_code = re.sub(r'def\s+main', 'def ' + feature_name, python_code)
    file_path = module_path + '/' + str(feature_name).strip() + '.py'
    sys.path.insert(0, file_path)
    python_code = python_code.replace("r@rawstringstart'", '')
    python_code = python_code.replace("'@rawstringend", '')
    # print(file_path,"===========")
    with open(file_path, 'w') as f:
        f.write(python_code)
    # print(feature_name)
    # module = import_module(feature_name)
    module = import_file(file_path)
    # print(feature_name, '=====', module)
    data = getattr(module, str(feature_name).strip())
    # print(data)
    executableoutput = data(source_code)
    # print(executableoutput)
    return Response(executableoutput, status=status.HTTP_200_OK)


# except Exception as err:
#     print(err)
#     return Response(err, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET'])
def miglevelobjects(request, migtypeid):
    objecttypes = ['Procedure', 'Function', 'Package', 'Index', 'Materialized view', 'Sequence', 'Synonym', 'Tabel',
                   'Trigger', 'Type', 'View']
    data_format_main = {}

    for index, i in enumerate(objecttypes):
        data_format = {}
        features = Feature.objects.filter(
            Object_Type=i, Migration_TypeId=migtypeid)
        serializer = migrationlevelfeatures(features, many=True)
        data = serializer.data
        if i == 'Index':
            lablename = i + 'es'
        else:
            lablename = i + 's'
        data_format['Label'] = lablename
        data_format['subMenu'] = data
        data_format_main[index + 1] = data_format
    datavalues = data_format_main.values()
    return Response(datavalues, status=status.HTTP_200_OK)


@api_view(['GET'])
def attachentsqlcodefiles(request, id):
    # data = Attachments.objects.filter(Feature_Id=Feature_Id,
    #                                       AttachmentType='Sourcecode') | Attachments.objects.filter(
    #     Feature_Id=Feature_Id, AttachmentType='Actualtargetcode') | Attachments.objects.filter(
    #     Feature_Id=Feature_Id, AttachmentType='Expectedconversion')
    data = Attachments.objects.all()
    serializer = AttachementSerializer(data, many=True)
    filenames = []
    dict1 = {}
    result = []
    for x in serializer.data:
        filenames.append(x['filename'])
        # print(x['File_name'], x['AttachmentType'])
    filenames = list(set(filenames))
    # print(filenames)
    for x in filenames:
        temp = {}
        temp['filename'] = x
        data1 = Attachments.objects.filter(
            Feature_Id=id, filename=x, AttachmentType='Sourcecode')

        a = list(data1.values_list())
        print(a)
        if len(a) == 0:
            temp['Sourcecode'] = 'N'
            # temp['sid'] = a[0]
        else:
            temp['Sourcecode'] = 'Y'
            temp['sid'] = a[0][0]
        data1 = Attachments.objects.filter(
            Feature_Id=id, filename=x, AttachmentType='Actualtargetcode')
        print(data1)
        a = list(data1.values_list())
        print(a)
        if len(a) == 0:
            temp['Actualtargetcode'] = 'N'
            # temp['atid'] = a[0]
        else:
            temp['Actualtargetcode'] = 'Y'
            temp['atid'] = a[0][0]
        data1 = Attachments.objects.filter(
            Feature_Id=id, filename=x, AttachmentType='Expectedconversion')
        a = list(data1.values_list())
        if len(a) == 0:
            temp['Expectedconversion'] = 'N'
            # temp['etid'] = a[0]
        else:
            temp['Expectedconversion'] = 'Y'
            temp['etid'] = a[0][0]

        if temp['Sourcecode'] == 'Y' or temp['Expectedconversion'] == 'Y' or temp['Actualtargetcode'] == 'Y':
            result.append(temp)
    return Response(result)


def create_and_append_sqlfile_single(path_of_file_sql, data):
    with open(path_of_file_sql, 'a') as f:
        f.write("{}\n\n\n\n".format(data))


@api_view(['POST'])
def feature_conversion_files(request):
    try:
        body_unicode = request.body.decode('utf-8')
        body_data = json.loads(body_unicode)
        feature_id = body_data['Feature_Id']
        attach_type = body_data['AttachmentType']
        feature = body_data['Feature_Name']
        feature1 = str(feature)[5:]
        migid = body_data['Migration_TypeId']
        objtype = body_data['Object_Type']
        path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        output_path = path + '/' + 'media' + '/' + migid + '/' + \
            objtype + '/' + feature + '/' + 'Actualtargetcode' + '/'
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        module_path = path + '/' + 'media' + '/' + migid + '/' + \
            objtype + '/' + feature + '/' + 'Conversion' + '/'
        sys.path.append(module_path)
        if not os.path.exists(module_path):
            return Response({"error": "Please upload Conversion Attachment before Converting into Files"},
                            status=status.HTTP_400_BAD_REQUEST)
        module_file = os.listdir(module_path)[0]
        # file_path = module_path + '/' + feature1 + '.py'
        file_path = module_path + '/' + module_file
        sys.path.insert(0, file_path)
        filter_files = Attachments.objects.filter(
            Feature_Id=feature_id, AttachmentType=attach_type)
        filter_values = list(filter_files.values_list())
        if filter_values:
            for file in filter_values:
                with open(file[4], 'r', encoding='utf-8') as f:
                    read_text = f.read()
                a = import_file(file_path)
                function_call = getattr(a, str(feature1).strip())
                output = function_call(read_text)
                if os.path.isfile(output_path + file[3]):
                    os.remove(output_path + file[3])
                create_and_append_sqlfile_single(output_path + file[3], output)
                target_filename = file[3]
                target_filepath = output_path + file[3]
                split_media = 'media' + target_filepath.split('media')[1]
                target_object = Attachments(AttachmentType='Actualtargetcode', filename=target_filename,
                                            Attachment=split_media, Feature_Id_id=feature_id)
                target_object.save()
        for row in Attachments.objects.all().reverse():
            if Attachments.objects.filter(filename=row.filename, AttachmentType=row.AttachmentType,
                                          Feature_Id_id=row.Feature_Id_id).count() > 1:
                row.delete()
        serializer = ConversionfilesSerializer(filter_files, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
    except Exception as err:
        return Response({"error": err}, status=status.HTTP_400_BAD_REQUEST)


class RegisterView(generics.GenericAPIView):
    serializer_class = RegisterSerializer

    def post(self, request):
        user = request.data
        serializer = self.serializer_class(data=user)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        user_data = serializer.data
        user = Users.objects.get(email=user_data['email'])
        token = RefreshToken.for_user(user)
        # current_site = get_current_site(request).domain
        # print(current_site,"current_site")
        # relativeLink = reverse('email-verify')
        # absurl = 'http://localhost:3000/' + current_site + relativeLink + "?token=" + str(token)

        absurl = frontend_url+'emailverification?' + str(token)
        # email_body = 'Hi ' + user.username + ' Use below link to verify your account \n' + absurl
        # data = {'email_body': email_body, 'to_email': user.email,
        #         'email_subject': 'Verify your email'}

        subject = 'Verify your email'
        html_message = render_to_string('verifys.html', {'url': absurl})
        plain_message = strip_tags(html_message)
        from_email = EMAIL_HOST_USER
        to = user.email
        mail.send_mail(subject, plain_message, from_email,
                       [to], html_message=html_message)
        # Util.send_email(data)
        return Response(user_data, status=status.HTTP_201_CREATED)


class ResendVerifyEmail(generics.GenericAPIView):
    serializer_class = resendemailserializer

    def post(self, request):
        user = request.data
        email = user['email']
        try:
            user = Users.objects.get(email=email)
            if user.is_verified:
                return Response({'msg': 'user is already verified'})
            token = RefreshToken.for_user(user)
            # current_site = get_current_site(request).domain
            # print(current_site,"current_site")
            # relativeLink = reverse('email-verify')

            # absurl = 'http://' + current_site + relativeLink + "?token=" + str(token)
            # email_body = 'Hi ' + user.username + 'Use below link to verify your account \n' + absurl
            # data = {'email_body': email_body, 'to_email': user.email, 'email_subject': 'Verify your email'}

            absurl = frontend_url+'emailverification?' + str(token)

            # email_body = 'Hi ' + user.username + ' Use below link to verify your account \n' + absurl
            # data = {'email_body': email_body, 'to_email': user.email,
            #         'email_subject': 'Verify your email'}
            # Util.send_email(data)

            subject = 'Verify your email'
            html_message = render_to_string('verifys.html', {'url': absurl})
            plain_message = strip_tags(html_message)
            from_email = EMAIL_HOST_USER
            to = user.email
            mail.send_mail(subject, plain_message, from_email,
                           [to], html_message=html_message)
            return Response({'msg': 'The Verification email has been sent Please Confirm'},
                            status=status.HTTP_201_CREATED)
        except:
            return Response({'msg': 'No Such user Please Register'})


class RequestPasswordResetEmail(generics.GenericAPIView):
    serializer_class = Resetpasswordemailserializer

    def post(self, request):
        # data = {'request' : request, 'data' : request.data}
        serializer = self.serializer_class(data=request.data)
        email = request.data['email']
        if Users.objects.filter(email=email).exists():
            user = Users.objects.get(email=email)
            uidb64 = urlsafe_base64_encode(smart_bytes(user.id))
            token = PasswordResetTokenGenerator().make_token(user)
            # current_site = get_current_site(request=request).domain
            # relativeLink = reverse('password-reset-confirm', kwargs={'uidb64': uidb64, 'token': token})
            # relativeLink = reverse('new_password')

            # absurl = 'http://' + current_site + relativeLink

            absurl = frontend_url+'resetpassword?token=' + \
                str(token) + "?uid=" + uidb64
            # email_body = 'Hello,  \n Use below link to reset your password \n' + absurl
            # data = {'email_body': email_body, 'to_email': user.email,
            #         'email_subject': 'Reset your password'}
            # Util.send_email(data)

            subject = 'Forgot Password'
            html_message = render_to_string(
                'forgotpassword.html', {'url': absurl})
            plain_message = strip_tags(html_message)
            from_email = EMAIL_HOST_USER
            to = user.email
            mail.send_mail(subject, plain_message, from_email,
                           [to], html_message=html_message)
            return Response({'msg': 'we have sent you a link to reset your password'},
                            status=status.HTTP_201_CREATED)
        else:
            return Response({'msg': 'No Such user Please Register'})

        return Response({'msg': 'we have sent you a link to reset your password'}, status=status.HTTP_200_OK)


class PasswordTokenCheckAPI(generics.GenericAPIView):
    def get(self, request, uidb64, token):

        try:
            id = smart_str(urlsafe_base64_decode(uidb64))
            user = Users.objects.get(id=id)
            if not PasswordResetTokenGenerator().check_token(user, token):
                return Response({'msg': 'Token is not valid, please request a new one'},
                                status=status.HTTP_401_UNAUTHORIZED)

            return Response({'success': True, 'msg': 'credentials valid', 'uidb64': uidb64, 'token': token},
                            status=status.HTTP_200_OK)

        except DjangoUnicodeDecodeError as identifier:
            return Response({'msg': 'Token is not valid, please request a new one'},
                            status=status.HTTP_401_UNAUTHORIZED)


class SetNewPasswordAPIView(generics.GenericAPIView):
    serializer_class = SetNewPasswordSerializer

    def patch(self, request):
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        return Response({'success': True, 'msg': 'Password Reset Success'}, status=status.HTTP_200_OK)


class VerifyEmail(generics.GenericAPIView):
    def get(self, request):
        token = request.GET.get('token')
        token = token.replace('?', '').strip()
        # token  = request.data['token']
        try:
            payload = jwt.decode(
                token, settings.SECRET_KEY, algorithms=['HS256'])
            user = Users.objects.get(id=payload['user_id'])
            if not user.is_verified:
                user.is_verified = True
                user.is_active = True
                user.save()
            return Response({'msg': 'Sucessfully Email Confirmed! Please Login'}, status=status.HTTP_200_OK)
        except jwt.ExpiredSignatureError as identifier:
            return Response({'msg': 'Expired Please Resend Email'}, status=status.HTTP_400_BAD_REQUEST)
        except jwt.exceptions.DecodeError as identifier:
            return Response({'msg': 'Invalid token'}, status=status.HTTP_400_BAD_REQUEST)


class MyObtainTokenPairView(TokenObtainPairView):
    permission_classes = (AllowAny,)
    serializer_class = MyTokenObtainPairSerializer


@api_view(['GET'])
def featurelistperuser(request):
    features = Users.objects.filter(username='naga')
    serializer = viewlevelfeatures(features, many=True)
    data = serializer.data
    # print(type(data[0]))
    data1 = data[0]
    data2 = eval(data1['can_view'])
    return Response(data2)


@api_view(['GET'])
def add_view(request):
    object_type = 'Function'
    # , '', 'Sequence', 'Synonym', 'Tabel','Trigger', 'Type', 'View'
    dict1 = {'Procedures': 0, 'Function': 1,
             'Package': 2, 'Index': 3, 'Materialized view': 4}
    index_num = dict1[object_type]
    feature = 'xml5'
    # print(request.Users.username,"username")
    query1 = Users.objects.filter(username='tejaaaa').values('can_view')
    temp = list(query1)
    temp = list(temp[0].values())
    temp = temp[0]
    temp1 = eval(temp)
    temp2 = temp1[index_num]
    temp3 = temp2['subMenu']
    feature_already = []
    for k in temp3:
        # print(k)
        a = list(k.values())
        feature_already.append(a[0])
    # print(feature_already)
    object_dict = {'Procedure': 'Proc', 'Function': 'Func', 'Package': 'Pack', 'Index': 'Inde',
                   'Materialized view': 'Mate', 'Sequence': 'Sequ', 'Synonym': 'Syno', 'Tabel': 'Tabe',
                   'Trigger': 'Trig', 'Type': 'Type', 'View': 'view'}
    Feature_Name = object_dict[object_type] + '_' + feature
    if feature not in feature_already:
        temp_dict = {
            "Feature_Name": Feature_Name
        }
        temp3.append(temp_dict)
    # print(temp1)
    a = Users.objects.get(username='tejaaaa')
    a.can_view = temp1
    a.save()
    return Response(temp1)


# @api_view(['GET'])
# # def sequence(request, Object_Type, Migration_TypeId):
# def Featurenames(request):
#     body_unicode = request.body.decode('utf-8')
#     body_data = json.loads(body_unicode)
#     Object_Type = body_data['Object_Type']
#     Sequence = body_data['']
#     Keywords = body_data['Keywords']
#     Estimations = body_data['Estimations']
#     features = Feature.objects.filter(Object_Type=Object_Type, Sequence=Sequence, Keywords=Keywords, Estimations=Estimations)
#     serializer = NameSerialixzer(features, many=True)
#     return Response(serializer.data, status=status.HTTP_200_OK)
#
# @api_view(['GET'])
# def Only_Featurenames(request):
#     features = Feature.objects.all()
#     serializer = NameSerialixzer(features, many=True)
#     return Response(serializer.data, status=status.HTTP_200_OK)


@api_view(['POST'])
def create_tablepage_featuresdata(request):
    # print(request)
    Migration_TypeId = request.data['Migration_TypeId']
    Object_Type = request.data['Object_Type']
    data = Feature.objects.filter(
        Migration_TypeId=Migration_TypeId, Object_Type=Object_Type)
    serializer = FeatureSerializer(data, many=True)
    return Response(serializer.data)


# @api_view(['POST'])
# def get_Featurenames(request):
#     # body_unicode = request.body.decode('utf-8')
#     # body_data = json.loads(body_unicode)
#     Migration_TypeId = request.data['Migration_TypeId']
#     Object_Type = request.data['Object_Type']
#     if str(Object_Type).upper() == 'ALL':
#         features = Feature.objects.all()
#     else:
#         features = Feature.objects.filter(
#             Migration_TypeId=Migration_TypeId, Object_Type=Object_Type)
#     serializer = migrationlevelfeatures(features, many=True)
#     return Response(serializer.data, status=status.HTTP_200_OK)


# @api_view(['POST'])
# def get_Featurenames(request):
#     # body_unicode = request.body.decode('utf-8')
#     # body_data = json.loads(body_unicode)
#     Migration_TypeId = request.data['Migration_TypeId']
#     Object_Type = request.data['Object_Type']
#     # if str(Object_Type).upper() == 'ALL':
#     #     features = Feature.objects.all()
#     # else:
#     features = Feature.objects.filter(Migration_TypeId=Migration_TypeId, Object_Type=Object_Type)
#     a = features.values()
#     # print(a)
#     new = []
#     for i in a:
#         new.append(i['Feature_Name'])
#     # print(new)
#     # print(str(Object_Type[0:4]) + "_" + "All")
#     # if str(Object_Type[0:4]) + "_" + "All" not in new:
#     #     query = Feature.objects.create(Migration_TypeId=Migration_TypeId, Object_Type=Object_Type, Feature_Name="All")
#     # #     query.save()
#     # # else:
#     # #     print("record With All is present")
#     serializer = migrationlevelfeatures(features, many=True)
#     return Response(serializer.data, status=status.HTTP_200_OK)


@api_view(['POST'])
def get_Featurenames(request):
    # body_unicode = request.body.decode('utf-8')
    # body_data = json.loads(body_unicode)
    Migration_TypeId = request.data['Migration_TypeId']
    Object_Type = request.data['Object_Type']#.upper()
    # print(Object_Type)
    Feature_Name = request.data['Feature_Name']#.upper()
    # print(Feature_Name)
    # if str(Object_Type).upper() == 'ALL':
    #     features = Feature.objects.all()
    # else:
    if Object_Type == 'ALL':
        features = Feature.objects.filter(Migration_TypeId=Migration_TypeId)

    elif Feature_Name == 'ALL':
        features = Feature.objects.filter(Migration_TypeId=Migration_TypeId, Object_Type=Object_Type)

    else:
        features = Feature.objects.filter(Migration_TypeId=Migration_TypeId, Object_Type=Object_Type)

    serializer = migrationlevelfeatures(features, many=True)
    return Response(serializer.data, status=status.HTTP_200_OK)

# @api_view(['POST'])
# def migrationsscreate(request):
#     migration_type = request.data['Migration_TypeId']
#     serializer = migrationcreateserializer(data=request.data)
#     if serializer.is_valid():
#         serializer.save(Code=migration_type.replace(' ', '_'))
#         return Response(serializer.data, status=status.HTTP_201_CREATED)
#     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

@api_view(['POST'])
def migrationsscreate(request):
    migration_type = request.data['Migration_TypeId']
    Object_Type = request.data['Object_Type']
    serializer = migrationcreateserializer(data=request.data)
    check_obj_type = migrations.objects.filter(Migration_TypeId=migration_type,Object_Type=Object_Type.upper())
    if not check_obj_type:
        if serializer.is_valid():
            serializer.save(Code=migration_type.replace(' ', '_'))
            serializer.save(Object_Type=Object_Type.upper())
            return Response(serializer.data, status=status.HTTP_201_CREATED)
    else:
        return Response('Object Type Already Existed')
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

@api_view(['GET'])
def migrationviewlist(request):
    features = migrations.objects.values('Migration_TypeId', 'Code').distinct()
    serializer = migrationviewserializer(features, many=True)
    return Response(serializer.data)


# @api_view(['POST'])
# def objectviewtlist(request):
#     Migration_TypeId = request.data['Migration_TypeId']
#     # print(Migration_TypeId)
#     features = migrations.objects.filter(
#         Migration_TypeId=Migration_TypeId).exclude(Object_Type="")
#     # print(features)
#     serializer = objectviewserializer(features, many=True)
#     return Response(serializer.data)

@api_view(['POST'])
def objectviewtlist(request):
    Migration_TypeId = request.data['Migration_TypeId']
    features = migrations.objects.filter(Migration_TypeId=Migration_TypeId).exclude(Object_Type="")
    # a = features.values()
    # new = []
    # for i in a:
    #     new.append(i['Object_Type'])
    # if "All" not in new:
    #     query = migrations.objects.create(Migration_TypeId=Migration_TypeId, Code = Migration_TypeId.replace(' ', '_'), Object_Type="All")
    #     query.save()
    serializer = objectviewserializer(features, many=True)
    return Response(serializer.data)


# @api_view(['POST'])
# def approvalscreate(request):
#     serializer = ApprovalSerializer(data=request.data)
#     if serializer.is_valid():
#         serializer.save()
#         return Response(serializer.data, status=status.HTTP_201_CREATED)
#     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# @api_view(['POST'])
# def approvalscreate(request):
#     # print(request.data)
#     User_Email = request.data['User_Email']
#     Object_Type = request.data['Object_Type']
#     Feature_Name = request.data['Feature_Name']
#     Access_Type = request.data['Access_Type']

#     if Approvals.objects.filter(User_Email=User_Email, Object_Type=Object_Type, Feature_Name=Feature_Name, Access_Type=Access_Type).exists():
#         return Response("Request Already Sent")

#     else:
#         serializer = ApprovalSerializer(data=request.data)
#         if serializer.is_valid():
#             serializer.save()
#             return Response(serializer.data, status=status.HTTP_201_CREATED)
#         return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['POST'])
def approvalscreate(request):
    User_Email = request.data['User_Email']
    Object_Type = request.data['Object_Type']
    Feature_Name = request.data['Feature_Name']
    Access_Type = request.data['Access_Type']
    if Approvals.objects.filter(User_Email=User_Email, Object_Type=Object_Type, Feature_Name=Feature_Name, Access_Type=Access_Type).exists():
        return Response("Request Already Sent")
    else:
        serializer = ApprovalSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


# @api_view(['GET','POST'])
# def approvalslist(request):
#     Migration_TypeId = request.data['Migration_TypeId']
#     today = date.today()
#     week_ago = today - timedelta(days=7)
#     final_list = []
#     appr_data = Approvals.objects.filter(Migration_TypeId=Migration_TypeId)
#     for dict in appr_data.values():
#         created_date = dict['Created_at']
#         if created_date > week_ago:
#             final_list.append(dict)
#     serializer = ApprovalSerializer(final_list, many=True)
#     return Response(serializer.data)


@api_view(['GET','POST'])
def approvalslist(request):
    Migration_TypeId = request.data['Migration_TypeId']
    Object_Type = request.data['Object_Type']
    today = date.today()
    week_ago = today - timedelta(days=7)
    final_list = []
    appr_data = Approvals.objects.filter(Migration_TypeId=Migration_TypeId, Object_Type=Object_Type)
    for dict in appr_data.values():
        created_date = dict['Created_at']
        if created_date > week_ago:
            final_list.append(dict)
    serializer = ApprovalSerializer(final_list, many=True)
    return Response(serializer.data)



@api_view(['GET'])
def userslist(request):
    features = Users.objects.filter(is_verified=True)
    serializer = usersserializer(features, many=True)
    return Response(serializer.data)


# @api_view(['POST'])
# def permissionscreate(request):
#     serializer = PermissionSerializer(data=request.data)
#     if serializer.is_valid():
#         serializer.save()
#         return Response(serializer.data, status=status.HTTP_201_CREATED)
#     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


# @api_view(['POST'])
# def permissionscreate(request):
#     email = request.data['User_Email']
#     ad_email = request.data['Approved_by']
#     mig_type = request.data['Migration_TypeId']
#     obj_type = request.data['Object_Type']
#     feature_name = request.data['Feature_Name']
#     created_date = request.data['Created_at']
#     expiry_date = request.data['Expiry_date']
#     request_access = request.data['Access_Type']
#     if request_access == 'Edit':
#         perm_data = Permissions.objects.filter(User_Email = email, Migration_TypeId = mig_type, Object_Type = obj_type,Feature_Name = feature_name,Access_Type = 'View' )
#         if not perm_data.values():
#             perm_view_data = Permissions.objects.create(User_Email = email,Migration_TypeId = mig_type, Object_Type = obj_type,Feature_Name = feature_name,Access_Type = 'View',Approved_by = ad_email,Created_at = created_date,Expiry_date = expiry_date )
#             perm_view_data.save()
#     serializer = PermissionSerializer(data=request.data)
#     if serializer.is_valid():
#         serializer.save()
#         return Response(serializer.data, status=status.HTTP_201_CREATED)
#     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


# @api_view(['POST'])
# def permissionscreate(request):
#     email = request.data['User_Email']
#     ad_email = request.data['Approved_by']
#     mig_type = request.data['Migration_TypeId']
#     obj_type = request.data['Object_Type']
#     feature_name = request.data['Feature_Name']
#     created_date = request.data['Created_at']
#     expiry_date = request.data['Expiry_date']
#     request_access = request.data['Access_Type']
#     if request_access == 'Edit':
#         perm_data = Permissions.objects.filter(User_Email = email, Migration_TypeId = mig_type, Object_Type = obj_type,Feature_Name = feature_name,Access_Type = 'View' )
#         if not perm_data.values():
#             perm_view_data = Permissions.objects.create(User_Email = email,Migration_TypeId = mig_type, Object_Type = obj_type,Feature_Name = feature_name,Access_Type = 'View',Approved_by = ad_email,Created_at = created_date,Expiry_date = expiry_date )
#             perm_view_data.save()
#     perm_record = Permissions.objects.filter(User_Email = email, Migration_TypeId = mig_type, Object_Type = obj_type,Feature_Name = feature_name,Access_Type =request_access)
#     if not perm_record:
#         serializer = PermissionSerializer(data=request.data)
#         if serializer.is_valid():
#             serializer.save()
#             return Response(serializer.data, status=status.HTTP_201_CREATED)
#         return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
#     else:
#         return Response("User already has permission")

# @api_view(['POST','GET'])
# def permissionscreate(request):
#     email = request.data['User_Email']
#     ad_email = request.data['Approved_by']
#     mig_type = request.data['Migration_TypeId']
#     obj_type = request.data['Object_Type']
#     feature_name = request.data['Feature_Name']
#     created_date = request.data['Created_at']
#     expiry_date = request.data['Expiry_date']
#     request_access = request.data['Access_Type']
#     if request_access == 'ALL':
#         feature = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,
#                                              Object_Type=obj_type, Feature_Name=feature_name,
#                                              Access_Type='View')
#         feature1 = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,
#                                               Object_Type=obj_type, Feature_Name=feature_name,
#                                               Access_Type='Edit')
#         feature.delete()
#         feature1.delete()
#         if obj_type == 'ALL':
#             perm_data_view = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,
#                                                       Access_Type='View')
#             perm_data_edit = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,
#                                                       Access_Type='Edit')
#             perm_data_view.delete()
#             perm_data_edit.delete()
#         elif feature_name == 'ALL' and obj_type != 'ALL':
#             perm_data_view = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,Object_Type = obj_type,
#                                                         Access_Type='View')
#             perm_data_edit = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,Object_Type = obj_type,
#                                                         Access_Type='Edit')
#             perm_data_view.delete()
#             perm_data_edit.delete()
#     if request_access == 'Edit':
#         feature = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,
#                                              Object_Type=obj_type, Feature_Name=feature_name,
#                                              Access_Type='View')
#         if feature:
#             feature.delete()
#         if obj_type == 'ALL':
#             perm_data_view = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,
#                                                       Access_Type='View')
#             perm_data_view.delete()
#         elif feature_name == 'ALL' and obj_type != 'ALL':
#             perm_data_view = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,Object_Type = obj_type,
#                                                         Access_Type='View')
#             perm_data_view.delete()
#     perm_record = Permissions.objects.filter(User_Email = email, Migration_TypeId = mig_type, Object_Type = obj_type,Feature_Name = feature_name,Access_Type =request_access)
#     if not perm_record:
#         serializer = PermissionSerializer(data=request.data)
#         if serializer.is_valid():
#             serializer.save()
#             return Response(serializer.data, status=status.HTTP_201_CREATED)
#         return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
#     else:
#         return Response("User already has permission")


@api_view(['POST','GET'])
def permissionscreate(request):
    user_Email = request.data['User_Email']
    mig_type = request.data['Migration_TypeId']
    obj_type = request.data['Object_Type']
    feature_name = request.data['Feature_Name']
    access_type = request.data['Access_Type']
    appr_status = request.data['Approval_Status']
    if appr_status == 'Approved':
        if obj_type != 'ALL' and feature_name == 'ALL' and access_type == 'View':
            perm_data = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type, Object_Type=obj_type,
                                                Access_Type='View').exclude(Feature_Name='ALL')
            if perm_data:
                perm_data.delete()
        elif obj_type != 'ALL' and feature_name == 'ALL' and access_type == 'Edit':
            perm_data_view = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,Access_Type='View')
            if perm_data_view:
                perm_data_view.delete()
            perm_data_edit = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,Access_Type='Edit').exclude(Feature_Name='ALL')
            if perm_data_edit:
                perm_data_edit.delete()
        elif obj_type != 'ALL' and feature_name == 'ALL' and access_type == 'ALL':
            perm_data_view = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,Access_Type='View')
            if perm_data_view:
                perm_data_view.delete()
            perm_data_edit = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,Access_Type='Edit')
            if perm_data_edit:
                perm_data_edit.delete()
            perm_data_all = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                    Object_Type=obj_type,Access_Type='ALL').exclude(Feature_Name='ALL')
            if perm_data_all:
                perm_data_all.delete()
        elif obj_type == 'ALL' and access_type == 'View':
            perm_data = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                Access_Type='View').exclude(Object_Type='ALL', Feature_Name='ALL')
            if perm_data:
                perm_data.delete()
        elif obj_type == 'ALL' and access_type == 'Edit':
            perm_data_view = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Access_Type='View')
            if perm_data_view:
                perm_data_view.delete()
            perm_data_edit = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Access_Type='Edit').exclude(Object_Type='ALL', Feature_Name='ALL')
            if perm_data_edit:
                perm_data_edit.delete()
        elif obj_type == 'ALL' and access_type == 'ALL':
            perm_data_view = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Access_Type='View')
            if perm_data_view:
                perm_data_view.delete()
            perm_data_edit = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Access_Type='Edit')
            if perm_data_edit:
                perm_data_edit.delete()
            perm_data_all = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                    Access_Type='ALL').exclude(Object_Type='ALL', Feature_Name='ALL')
            if perm_data_all:
                perm_data_all.delete()
        elif access_type == 'Edit':
            perm_data = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type, Object_Type=obj_type,
                                                Feature_Name=feature_name, Access_Type='View')
            if perm_data:
                perm_data.delete()
        elif access_type == 'ALL':
            perm_data_view = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,Feature_Name=feature_name, Access_Type='View')
            perm_data_edit = Permissions.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,Feature_Name=feature_name, Access_Type='Edit')
            if perm_data_view:
                perm_data_view.delete()
            if perm_data_edit:
                perm_data_edit.delete()

    perm_record = Permissions.objects.filter(User_Email = user_Email, Migration_TypeId = mig_type, Object_Type = obj_type,Feature_Name = feature_name,Access_Type =access_type)
    if not perm_record:
        serializer = PermissionSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    else:
        return Response("User already has permission")


# @api_view(['POST'])
# def migration_user_view(request):
#     email = request.data['uemail']
#     mig_type = request.data['Migration_TypeId']
#     user = Users.objects.get(email=email)
#     mig_data = migrations.objects.filter(
#         Migration_TypeId=mig_type).exclude(Object_Type='')
#     serializer = migrationformatserializer(mig_data, many=True)
#     object_names = [obj['Object_Type'] for obj in serializer.data]

#     label_dict = {}
#     final_list = []

#     perm_data = Permissions.objects.filter(
#         User_Email=email, Migration_TypeId=mig_type, Access_Type='View')
#     type_objects = list(perm_data.values('Object_Type').distinct())
#     feature_value_object = list(perm_data.values('Feature_Name'))
#     object_values_list = [
#         value for elem in type_objects for value in elem.values()]
#     feature_value_list = [
#         value for elem in feature_value_object for value in elem.values()]

#     if 'All' in object_values_list or user.is_superuser:
#         for object_name in object_names:
#             inter_list = []
#             label_dict['Label'] = object_name
#             features_data = Feature.objects.filter(
#                 Migration_TypeId=mig_type, Object_Type=object_name)
#             features_objects = list(features_data.values('Feature_Name'))
#             for obj_feature in features_objects:
#                 feature_name = obj_feature['Feature_Name']
#                 data_features = Feature.objects.filter(
#                     Feature_Name=feature_name)
#                 feature_object_list = list(data_features.values('Feature_Id'))
#                 feature_id = feature_object_list[0]['Feature_Id']

#                 inter_dict = {}
#                 inter_dict["Feature_Id"] = feature_id
#                 inter_dict["Feature_Name"] = feature_name
#                 inter_list.append(inter_dict)
#             label_dict['SubMenu'] = inter_list
#             final_list.append(label_dict.copy())
#     else:
#         if 'All' in feature_value_list:
#             perm_data1 = Permissions.objects.filter(
#                 User_Email=email, Migration_TypeId=mig_type, Access_Type='View', Feature_Name='All')
#             all_type_objects = list(
#                 perm_data1.values('Object_Type').distinct())
#             all_type_objects_list = [
#                 value for elem in all_type_objects for value in elem.values()]
#             for object_name in object_names:
#                 inter_list = []
#                 label_dict['Label'] = object_name
#                 if object_name in all_type_objects_list:
#                     features_data = Feature.objects.filter(
#                         Migration_TypeId=mig_type, Object_Type=object_name)
#                     features_objects = list(
#                         features_data.values('Feature_Name'))
#                     for obj_feature in features_objects:
#                         feature_name = obj_feature['Feature_Name']
#                         data_features = Feature.objects.filter(
#                             Feature_Name=feature_name)
#                         feature_object_list = list(
#                             data_features.values('Feature_Id'))
#                         feature_id = feature_object_list[0]['Feature_Id']

#                         inter_dict = {}
#                         inter_dict["Feature_Id"] = feature_id
#                         inter_dict["Feature_Name"] = feature_name
#                         inter_list.append(inter_dict)
#                     label_dict['SubMenu'] = inter_list
#                     final_list.append(label_dict.copy())
#                 else:
#                     label_dict['SubMenu'] = []
#                     final_list.append(label_dict.copy())
#         else:
#             for object_name in object_names:
#                 inter_list = []
#                 label_dict['Label'] = object_name
#                 for feature_name in feature_value_list:
#                     perm_data2 = Permissions.objects.filter(
#                         User_Email=email, Migration_TypeId=mig_type, Object_Type=object_name, Access_Type='View', Feature_Name=feature_name)
#                     features_objects = list(perm_data2.values('Feature_Name'))
#                     for obj_feature in features_objects:
#                         feature_name = obj_feature['Feature_Name']
#                         data_features = Feature.objects.filter(
#                             Feature_Name=feature_name)
#                         feature_object_list = list(
#                             data_features.values('Feature_Id'))
#                         feature_id = feature_object_list[0]['Feature_Id']

#                         inter_dict = {}
#                         inter_dict["Feature_Id"] = feature_id
#                         inter_dict["Feature_Name"] = feature_name
#                         inter_list.append(inter_dict)
#                 label_dict['SubMenu'] = inter_list
#                 final_list.append(label_dict.copy())

#     return Response(final_list)

# @api_view(['POST'])
# def migration_user_view(request):
#     # email = request.data['uemail']
#     email = request.data['User_Email']
#     mig_type = request.data['Migration_TypeId']
#     user = Users.objects.filter(email=email)
#     mig_data = migrations.objects.filter(
#         Migration_TypeId=mig_type).exclude(Object_Type='')
#     serializer = migrationformatserializer(mig_data, many=True)
#     object_names = [obj['Object_Type'] for obj in serializer.data]
#
#     label_dict = {}
#     final_list = []
#     #commented by saiteja
#     # perm_data = Permissions.objects.filter(
#     #     User_Email=email, Migration_TypeId=mig_type, Access_Type='View')
#     perm_data = Permissions.objects.filter(
#         User_Email=email, Migration_TypeId=mig_type)
#     type_objects = list(perm_data.values('Object_Type').distinct())
#     feature_value_object = list(perm_data.values('Feature_Name'))
#     object_values_list = [
#         value for elem in type_objects for value in elem.values()]
#     feature_value_list = [
#         value for elem in feature_value_object for value in elem.values()]
#
#     user_values = list(user.values())
#     user_is_superuser = user_values[0]['is_superuser']
#     user_admin_list = user_values[0]['admin_migrations']
#     if user_admin_list != None:
#         user_admin_list = user_admin_list.split('.')
#         user_admin_list = [x for x in user_admin_list if x]
#     else :
#         user_admin_list = []
#
#     if 'ALL' in object_values_list or user_is_superuser == True or mig_type in user_admin_list:
#         for object_name in object_names:
#             inter_list = []
#             label_dict['Label'] = object_name
#             features_data = Feature.objects.filter(
#                 Migration_TypeId=mig_type, Object_Type=object_name)
#             features_objects = list(features_data.values('Feature_Name'))
#             for obj_feature in features_objects:
#                 feature_name = obj_feature['Feature_Name']
#                 data_features = Feature.objects.filter(
#                     Feature_Name=feature_name)
#                 feature_object_list = list(data_features.values('Feature_Id'))
#                 feature_id = feature_object_list[0]['Feature_Id']
#
#                 inter_dict = {}
#                 inter_dict["Feature_Id"] = feature_id
#                 inter_dict["Feature_Name"] = feature_name
#                 inter_list.append(inter_dict)
#             label_dict['SubMenu'] = inter_list
#             final_list.append(label_dict.copy())
#     else:
#         if 'ALL' in feature_value_list:
#             perm_data1 = Permissions.objects.filter(
#                 User_Email=email, Migration_TypeId=mig_type, Feature_Name='ALL')
#             all_type_objects = list(
#                 perm_data1.values('Object_Type').distinct())
#             all_type_objects_list = [
#                 value for elem in all_type_objects for value in elem.values()]
#             for object_name in object_names:
#                 inter_list = []
#                 label_dict['Label'] = object_name
#                 if object_name in all_type_objects_list:
#                     features_data = Feature.objects.filter(
#                         Migration_TypeId=mig_type, Object_Type=object_name)
#                     features_objects = list(
#                         features_data.values('Feature_Name'))
#                     for obj_feature in features_objects:
#                         feature_name = obj_feature['Feature_Name']
#                         data_features = Feature.objects.filter(
#                             Feature_Name=feature_name)
#                         feature_object_list = list(
#                             data_features.values('Feature_Id'))
#                         feature_id = feature_object_list[0]['Feature_Id']
#
#                         inter_dict = {}
#                         inter_dict["Feature_Id"] = feature_id
#                         inter_dict["Feature_Name"] = feature_name
#                         inter_list.append(inter_dict)
#                     label_dict['SubMenu'] = inter_list
#                     final_list.append(label_dict.copy())
#                 else:
#                     label_dict['SubMenu'] = []
#                     final_list.append(label_dict.copy())
#         else:
#             for object_name in object_names:
#                 inter_list = []
#                 label_dict['Label'] = object_name
#                 for feature_name in feature_value_list:
#                     perm_data2 = Permissions.objects.filter(
#                         User_Email=email, Migration_TypeId=mig_type, Object_Type=object_name,  Feature_Name=feature_name)
#                     features_objects = list(perm_data2.values('Feature_Name'))
#                     for obj_feature in features_objects:
#                         feature_name = obj_feature['Feature_Name']
#                         data_features = Feature.objects.filter(
#                             Feature_Name=feature_name)
#                         feature_object_list = list(
#                             data_features.values('Feature_Id'))
#                         feature_id = feature_object_list[0]['Feature_Id']
#
#                         inter_dict = {}
#                         inter_dict["Feature_Id"] = feature_id
#                         inter_dict["Feature_Name"] = feature_name
#                         inter_list.append(inter_dict)
#                 label_dict['SubMenu'] = inter_list
#                 final_list.append(label_dict.copy())
#
#     return Response(final_list)


# @api_view(['POST'])
# def migration_user_view(request):
#     email = request.data['User_Email']
#     mig_type = request.data['Migration_TypeId']
#
#     user = Users.objects.filter(email=email)
#     user_is_superuser = user.values()[0]['is_superuser']
#     user_admin_list = user.values()[0]['admin_migrations']
#     if user_admin_list != None:
#         user_admin_list = user_admin_list.split(',')
#         user_admin_list = [x for x in user_admin_list if x]
#     else:
#         user_admin_list = []
#
#     mig_data =  migrations.objects.filter(Migration_TypeId=mig_type).exclude(Object_Type='')
#     object_names = [obj['Object_Type'] for obj in mig_data.values()]
#
#     perm_data = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type)
#     object_values_list = [obj['Object_Type'] for obj in perm_data.values()]
#
#     label_dict = {}
#     final_list = []
#
#     if user_is_superuser == True or mig_type in user_admin_list or 'ALL' in object_values_list:
#         for object_name in object_names:
#             inter_list = []
#             label_dict['Label'] = object_name
#             features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
#             feature_values = [obj['Feature_Name'] for obj in features_data.values()]
#             if feature_values:
#                 for feature_name in feature_values:
#                     feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name,Feature_Name=feature_name)
#                     feature_id = feature_values_data.values()[0]['Feature_Id']
#                     inter_dict = {}
#                     inter_dict["Feature_Id"] = feature_id
#                     inter_dict["Feature_Name"] = feature_name
#                     inter_list.append(inter_dict)
#                 label_dict['SubMenu'] = inter_list
#                 final_list.append(label_dict.copy())
#             else:
#                 label_dict['SubMenu'] = []
#                 final_list.append(label_dict.copy())
#     else:
#         for object_name in object_names:
#             inter_list = []
#             label_dict['Label'] = object_name
#             if object_name in object_values_list:
#                 perm_data = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,Object_Type = object_name)
#                 feature_names_list = [obj['Feature_Name'] for obj in perm_data.values()]
#                 if 'ALL' in feature_names_list:
#                     features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
#                     feature_values = [obj['Feature_Name'] for obj in features_data.values()]
#                     if feature_values:
#                         for feature_name in feature_values:
#                             feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
#                                                                          Object_Type=object_name,
#                                                                          Feature_Name=feature_name)
#                             feature_id = feature_values_data.values()[0]['Feature_Id']
#                             inter_dict = {}
#                             inter_dict["Feature_Id"] = feature_id
#                             inter_dict["Feature_Name"] = feature_name
#                             inter_list.append(inter_dict)
#                         label_dict['SubMenu'] = inter_list
#                         final_list.append(label_dict.copy())
#                     else:
#                         label_dict['SubMenu'] = []
#                         final_list.append(label_dict.copy())
#                 else:
#                     for feature_i in feature_names_list:
#                         features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name,Feature_Name = feature_i)
#                         feature_id = features_data.values()[0]['Feature_Id']
#                         inter_dict = {}
#                         inter_dict["Feature_Id"] = feature_id
#                         inter_dict["Feature_Name"] = feature_i
#                         inter_list.append(inter_dict)
#                     label_dict['SubMenu'] = inter_list
#                     final_list.append(label_dict.copy())
#
#     return Response(final_list)


# @api_view(['POST'])
# def migration_user_view(request):
#     email = request.data['User_Email']
#     mig_type = request.data['Migration_TypeId']
#     # print('mig_type', mig_type)
#
#     user = Users.objects.get(email=email)
#     user_is_superuser = user.is_superuser
#     admin_access = user.admin_migrations
#     if admin_access != '':
#         admin_access = admin_access.replace("\'", "\"")
#         admin_access_dict = json.loads(admin_access)
#     else:
#         admin_access_dict = {}
#
#     mig_data =  migrations.objects.filter(Migration_TypeId=mig_type).exclude(Object_Type='')
#     object_names = [obj['Object_Type'] for obj in mig_data.values()]
#
#     perm_data = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type)
#     object_values_list = [obj['Object_Type'] for obj in perm_data.values()]
#
#     label_dict = {}
#     final_list = []
#
#     if user_is_superuser == True or 'ALL' in object_values_list:
#         for object_name in object_names:
#             inter_list = []
#             label_dict['Label'] = object_name
#             features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
#             feature_values = [obj['Feature_Name'] for obj in features_data.values()]
#             if feature_values:
#                 for feature_name in feature_values:
#                     feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name,Feature_Name=feature_name)
#                     feature_id = feature_values_data.values()[0]['Feature_Id']
#                     inter_dict = {}
#                     inter_dict["Feature_Id"] = feature_id
#                     inter_dict["Feature_Name"] = feature_name
#                     inter_list.append(inter_dict)
#                 label_dict['SubMenu'] = inter_list
#                 label_dict['Admin_Flag'] = 0
#                 final_list.append(label_dict.copy())
#             else:
#                 label_dict['SubMenu'] = []
#                 label_dict['Admin_Flag'] = 0
#                 final_list.append(label_dict.copy())
#
#     elif mig_type in admin_access_dict.keys():
#         if 'ALL' in admin_access_dict[mig_type]:
#             for object_name in object_names:
#                 inter_list = []
#                 label_dict['Label'] = object_name
#                 features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
#                 feature_values = [obj['Feature_Name'] for obj in features_data.values()]
#                 if feature_values:
#                     for feature_name in feature_values:
#                         feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
#                                                                      Object_Type=object_name,
#                                                                      Feature_Name=feature_name)
#                         feature_id = feature_values_data.values()[0]['Feature_Id']
#                         inter_dict = {}
#                         inter_dict["Feature_Id"] = feature_id
#                         inter_dict["Feature_Name"] = feature_name
#                         inter_list.append(inter_dict)
#                     label_dict['SubMenu'] = inter_list
#                     label_dict['Admin_Flag'] = 1
#                     final_list.append(label_dict.copy())
#                 else:
#                     label_dict['SubMenu'] = []
#                     label_dict['Admin_Flag'] = 1
#                     final_list.append(label_dict.copy())
#         else:
#             for object_name in object_names:
#                 inter_list = []
#                 label_dict['Label'] = object_name
#                 if object_name in admin_access_dict[mig_type]:
#                     features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
#                     feature_values = [obj['Feature_Name'] for obj in features_data.values()]
#                     if feature_values:
#                         for feature_name in feature_values:
#                             feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name, Feature_Name=feature_name)
#                             feature_id = feature_values_data.values()[0]['Feature_Id']
#                             inter_dict = {}
#                             inter_dict["Feature_Id"] = feature_id
#                             inter_dict["Feature_Name"] = feature_name
#                             inter_list.append(inter_dict)
#                         label_dict['SubMenu'] = inter_list
#                         label_dict['Admin_Flag'] = 1
#                         final_list.append(label_dict.copy())
#                     else:
#                         label_dict['SubMenu'] = []
#                         label_dict['Admin_Flag'] = 1
#                         final_list.append(label_dict.copy())
#                 else:
#                     if object_name in object_values_list:
#                         perm_data = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,
#                                                                Object_Type=object_name)
#                         feature_names_list = [obj['Feature_Name'] for obj in perm_data.values()]
#                         if 'ALL' in feature_names_list:
#                             features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
#                             feature_values = [obj['Feature_Name'] for obj in features_data.values()]
#                             if feature_values:
#                                 for feature_name in feature_values:
#                                     feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
#                                                                                  Object_Type=object_name,
#                                                                                  Feature_Name=feature_name)
#                                     feature_id = feature_values_data.values()[0]['Feature_Id']
#                                     inter_dict = {}
#                                     inter_dict["Feature_Id"] = feature_id
#                                     inter_dict["Feature_Name"] = feature_name
#                                     inter_list.append(inter_dict)
#                                 label_dict['SubMenu'] = inter_list
#                                 label_dict['Admin_Flag'] = 0
#                                 final_list.append(label_dict.copy())
#                             else:
#                                 label_dict['SubMenu'] = []
#                                 label_dict['Admin_Flag'] = 0
#                                 final_list.append(label_dict.copy())
#                         else:
#                             for feature_i in feature_names_list:
#                                 features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name,
#                                                                        Feature_Name=feature_i)
#                                 feature_id = features_data.values()[0]['Feature_Id']
#                                 inter_dict = {}
#                                 inter_dict["Feature_Id"] = feature_id
#                                 inter_dict["Feature_Name"] = feature_i
#                                 inter_list.append(inter_dict)
#                             label_dict['SubMenu'] = inter_list
#                             label_dict['Admin_Flag'] = 0
#                             final_list.append(label_dict.copy())
#     else:
#         for object_name in object_names:
#             inter_list = []
#             label_dict['Label'] = object_name
#             if object_name in object_values_list:
#                 perm_data = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,Object_Type = object_name)
#                 feature_names_list = [obj['Feature_Name'] for obj in perm_data.values()]
#                 if 'ALL' in feature_names_list:
#                     features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
#                     feature_values = [obj['Feature_Name'] for obj in features_data.values()]
#                     if feature_values:
#                         for feature_name in feature_values:
#                             feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
#                                                                          Object_Type=object_name,
#                                                                          Feature_Name=feature_name)
#                             feature_id = feature_values_data.values()[0]['Feature_Id']
#                             inter_dict = {}
#                             inter_dict["Feature_Id"] = feature_id
#                             inter_dict["Feature_Name"] = feature_name
#                             inter_list.append(inter_dict)
#                         label_dict['SubMenu'] = inter_list
#                         label_dict['Admin_Flag'] = 0
#                         final_list.append(label_dict.copy())
#                     else:
#                         label_dict['SubMenu'] = []
#                         label_dict['Admin_Flag'] = 0
#                         final_list.append(label_dict.copy())
#                 else:
#                     for feature_i in feature_names_list:
#                         features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name,Feature_Name = feature_i)
#                         feature_id = features_data.values()[0]['Feature_Id']
#                         inter_dict = {}
#                         inter_dict["Feature_Id"] = feature_id
#                         inter_dict["Feature_Name"] = feature_i
#                         inter_list.append(inter_dict)
#                     label_dict['SubMenu'] = inter_list
#                     label_dict['Admin_Flag'] = 0
#                     final_list.append(label_dict.copy())
#
#     return Response(final_list)
#
#
@api_view(['GET','POST'])
def create_check_list(request):
    email = request.data['User_Email']
    mig_type = request.data['Migration_Type']

    mig_data = migrations.objects.filter(Migration_TypeId=mig_type).exclude(Object_Type='')
    object_names = [obj['Object_Type'] for obj in mig_data.values()]

    perm_object_list = []
    perm_data = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Access_Type = 'Edit') | Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type, Access_Type = 'ALL')
    object_data = perm_data.values('Object_Type')
    for dict in object_data:
        perm_object_list.append(dict['Object_Type'])
    perm_object_list = list(set(perm_object_list))

    inter_dict = {}
    final_list =[]
    if 'ALL' in perm_object_list:
        for object_name in object_names:
            inter_dict['Label'] =  object_name
            inter_dict['Create_Flag'] = 1
            final_list.append(inter_dict.copy())
    else:
        for object_name in object_names:
            if object_name in perm_object_list:
                inter_dict['Label'] = object_name
                inter_dict['Create_Flag'] = 1
                final_list.append(inter_dict.copy())
            else:
                inter_dict['Label'] = object_name
                inter_dict['Create_Flag'] = 0
                final_list.append(inter_dict.copy())
    return Response(final_list)


@api_view(['POST'])
def migration_user_view(request):
    email = request.data['User_Email']
    mig_type = request.data['Migration_TypeId']
    # print('mig_type=================', mig_type)

    user = Users.objects.get(email=email)
    user_is_superuser = user.is_superuser
    # print("user_is_superuser====",user_is_superuser)
    admin_access = user.admin_migrations
    if admin_access != '':
        admin_access = admin_access.replace("\'", "\"")
        admin_access_dict = json.loads(admin_access)
    else:
        admin_access_dict = {}

    mig_data = migrations.objects.filter(Migration_TypeId=mig_type).exclude(Object_Type='')
    object_names = [obj['Object_Type'] for obj in mig_data.values()]

    perm_data = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type)
    object_values_list = [obj['Object_Type'] for obj in perm_data.values()]

    label_dict = {}
    final_list = []
    if user_is_superuser == True :
        print(mig_type,"============",admin_access_dict.keys())
        if mig_type in admin_access_dict.keys():
            print("if if loop")
            if 'ALL' in admin_access_dict[mig_type]:
                print("if if if loop")
                for object_name in object_names:
                    inter_list = []
                    label_dict['Label'] = object_name
                    features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
                    feature_values = [obj['Feature_Name'] for obj in features_data.values()]
                    if feature_values:
                        for feature_name in feature_values:
                            feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
                                                                         Object_Type=object_name,
                                                                         Feature_Name=feature_name)
                            feature_id = feature_values_data.values()[0]['Feature_Id']
                            inter_dict = {}
                            inter_dict["Feature_Id"] = feature_id
                            inter_dict["Feature_Name"] = feature_name
                            inter_list.append(inter_dict)
                        label_dict['SubMenu'] = inter_list
                        label_dict['Admin_Flag'] = 1
                        final_list.append(label_dict.copy())
                    else:
                        label_dict['SubMenu'] = []
                        label_dict['Admin_Flag'] = 1
                        final_list.append(label_dict.copy())
            else:
                print("if if else loop")
                for object_name in object_names:
                    inter_list = []
                    label_dict['Label'] = object_name
                    if object_name in admin_access_dict[mig_type]:
                        features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
                        feature_values = [obj['Feature_Name'] for obj in features_data.values()]
                        if feature_values:
                            for feature_name in feature_values:
                                feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
                                                                             Object_Type=object_name,
                                                                             Feature_Name=feature_name)
                                feature_id = feature_values_data.values()[0]['Feature_Id']
                                inter_dict = {}
                                inter_dict["Feature_Id"] = feature_id
                                inter_dict["Feature_Name"] = feature_name
                                inter_list.append(inter_dict)
                            label_dict['SubMenu'] = inter_list
                            label_dict['Admin_Flag'] = 1
                            final_list.append(label_dict.copy())
                        else:
                            label_dict['SubMenu'] = []
                            label_dict['Admin_Flag'] = 1
                            final_list.append(label_dict.copy())
                    else:
                        if object_name in object_values_list:
                            perm_data = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,
                                                                   Object_Type=object_name)
                            feature_names_list = [obj['Feature_Name'] for obj in perm_data.values()]
                            if 'ALL' in feature_names_list:
                                features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
                                feature_values = [obj['Feature_Name'] for obj in features_data.values()]
                                if feature_values:
                                    for feature_name in feature_values:
                                        feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
                                                                                     Object_Type=object_name,
                                                                                     Feature_Name=feature_name)
                                        feature_id = feature_values_data.values()[0]['Feature_Id']
                                        inter_dict = {}
                                        inter_dict["Feature_Id"] = feature_id
                                        inter_dict["Feature_Name"] = feature_name
                                        inter_list.append(inter_dict)
                                    label_dict['SubMenu'] = inter_list
                                    label_dict['Admin_Flag'] = 0
                                    final_list.append(label_dict.copy())
                                else:
                                    label_dict['SubMenu'] = []
                                    label_dict['Admin_Flag'] = 0
                                    final_list.append(label_dict.copy())
                            else:
                                for feature_i in feature_names_list:
                                    features_data = Feature.objects.filter(Migration_TypeId=mig_type,
                                                                           Object_Type=object_name,
                                                                           Feature_Name=feature_i)
                                    feature_id = features_data.values()[0]['Feature_Id']
                                    inter_dict = {}
                                    inter_dict["Feature_Id"] = feature_id
                                    inter_dict["Feature_Name"] = feature_i
                                    inter_list.append(inter_dict)
                                label_dict['SubMenu'] = inter_list
                                label_dict['Admin_Flag'] = 0
                                final_list.append(label_dict.copy())
        else:
            print("if else loop")
            for object_name in object_names:
                inter_list = []
                label_dict['Label'] = object_name
                features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
                feature_values = [obj['Feature_Name'] for obj in features_data.values()]
                if feature_values:
                    for feature_name in feature_values:
                        feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name,Feature_Name=feature_name)
                        feature_id = feature_values_data.values()[0]['Feature_Id']
                        inter_dict = {}
                        inter_dict["Feature_Id"] = feature_id
                        inter_dict["Feature_Name"] = feature_name
                        inter_list.append(inter_dict)
                    label_dict['SubMenu'] = inter_list
                    label_dict['Admin_Flag'] = 0
                    final_list.append(label_dict.copy())
                else:
                    label_dict['SubMenu'] = []
                    label_dict['Admin_Flag'] = 0
                    final_list.append(label_dict.copy())
    elif 'ALL' in object_values_list :
        for object_name in object_names:
            inter_list = []
            label_dict['Label'] = object_name
            features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
            feature_values = [obj['Feature_Name'] for obj in features_data.values()]
            if feature_values:
                for feature_name in feature_values:
                    feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name,
                                                                 Feature_Name=feature_name)
                    feature_id = feature_values_data.values()[0]['Feature_Id']
                    inter_dict = {}
                    inter_dict["Feature_Id"] = feature_id
                    inter_dict["Feature_Name"] = feature_name
                    inter_list.append(inter_dict)
                label_dict['SubMenu'] = inter_list
                if object_name in admin_access_dict[mig_type]:
                    label_dict['Admin_Flag'] = 1
                else:
                    label_dict['Admin_Flag'] = 0
                final_list.append(label_dict.copy())
            else:
                label_dict['SubMenu'] = []
                if object_name in admin_access_dict[mig_type]:
                    label_dict['Admin_Flag'] = 1
                else:
                    label_dict['Admin_Flag'] = 0
                final_list.append(label_dict.copy())
    elif mig_type in admin_access_dict.keys():
        if 'ALL' in admin_access_dict[mig_type]:
            for object_name in object_names:
                inter_list = []
                label_dict['Label'] = object_name
                features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
                feature_values = [obj['Feature_Name'] for obj in features_data.values()]
                if feature_values:
                    for feature_name in feature_values:
                        feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
                                                                     Object_Type=object_name,
                                                                     Feature_Name=feature_name)
                        feature_id = feature_values_data.values()[0]['Feature_Id']
                        inter_dict = {}
                        inter_dict["Feature_Id"] = feature_id
                        inter_dict["Feature_Name"] = feature_name
                        inter_list.append(inter_dict)
                    label_dict['SubMenu'] = inter_list
                    label_dict['Admin_Flag'] = 1
                    final_list.append(label_dict.copy())
                else:
                    label_dict['SubMenu'] = []
                    label_dict['Admin_Flag'] = 1
                    final_list.append(label_dict.copy())
        else:
            for object_name in object_names:
                inter_list = []
                label_dict['Label'] = object_name
                if object_name in admin_access_dict[mig_type]:
                    features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
                    feature_values = [obj['Feature_Name'] for obj in features_data.values()]
                    if feature_values:
                        for feature_name in feature_values:
                            feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
                                                                         Object_Type=object_name,
                                                                         Feature_Name=feature_name)
                            feature_id = feature_values_data.values()[0]['Feature_Id']
                            inter_dict = {}
                            inter_dict["Feature_Id"] = feature_id
                            inter_dict["Feature_Name"] = feature_name
                            inter_list.append(inter_dict)
                        label_dict['SubMenu'] = inter_list
                        label_dict['Admin_Flag'] = 1
                        final_list.append(label_dict.copy())
                    else:
                        label_dict['SubMenu'] = []
                        label_dict['Admin_Flag'] = 1
                        final_list.append(label_dict.copy())
                else:
                    if object_name in object_values_list:
                        perm_data = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,
                                                               Object_Type=object_name)
                        feature_names_list = [obj['Feature_Name'] for obj in perm_data.values()]
                        if 'ALL' in feature_names_list:
                            features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
                            feature_values = [obj['Feature_Name'] for obj in features_data.values()]
                            if feature_values:
                                for feature_name in feature_values:
                                    feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
                                                                                 Object_Type=object_name,
                                                                                 Feature_Name=feature_name)
                                    feature_id = feature_values_data.values()[0]['Feature_Id']
                                    inter_dict = {}
                                    inter_dict["Feature_Id"] = feature_id
                                    inter_dict["Feature_Name"] = feature_name
                                    inter_list.append(inter_dict)
                                label_dict['SubMenu'] = inter_list
                                label_dict['Admin_Flag'] = 0
                                final_list.append(label_dict.copy())
                            else:
                                label_dict['SubMenu'] = []
                                label_dict['Admin_Flag'] = 0
                                final_list.append(label_dict.copy())
                        else:
                            for feature_i in feature_names_list:
                                features_data = Feature.objects.filter(Migration_TypeId=mig_type,
                                                                       Object_Type=object_name,
                                                                       Feature_Name=feature_i)
                                feature_id = features_data.values()[0]['Feature_Id']
                                inter_dict = {}
                                inter_dict["Feature_Id"] = feature_id
                                inter_dict["Feature_Name"] = feature_i
                                inter_list.append(inter_dict)
                            label_dict['SubMenu'] = inter_list
                            label_dict['Admin_Flag'] = 0
                            final_list.append(label_dict.copy())
    else:
        for object_name in object_names:
            inter_list = []
            label_dict['Label'] = object_name
            if object_name in object_values_list:
                perm_data = Permissions.objects.filter(User_Email=email, Migration_TypeId=mig_type,
                                                       Object_Type=object_name)
                feature_names_list = [obj['Feature_Name'] for obj in perm_data.values()]
                if 'ALL' in feature_names_list:
                    features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name)
                    feature_values = [obj['Feature_Name'] for obj in features_data.values()]
                    if feature_values:
                        for feature_name in feature_values:
                            feature_values_data = Feature.objects.filter(Migration_TypeId=mig_type,
                                                                         Object_Type=object_name,
                                                                         Feature_Name=feature_name)
                            feature_id = feature_values_data.values()[0]['Feature_Id']
                            inter_dict = {}
                            inter_dict["Feature_Id"] = feature_id
                            inter_dict["Feature_Name"] = feature_name
                            inter_list.append(inter_dict)
                        label_dict['SubMenu'] = inter_list
                        label_dict['Admin_Flag'] = 0
                        final_list.append(label_dict.copy())
                    else:
                        label_dict['SubMenu'] = []
                        label_dict['Admin_Flag'] = 0
                        final_list.append(label_dict.copy())
                else:
                    for feature_i in feature_names_list:
                        features_data = Feature.objects.filter(Migration_TypeId=mig_type, Object_Type=object_name,
                                                               Feature_Name=feature_i)
                        feature_id = features_data.values()[0]['Feature_Id']
                        inter_dict = {}
                        inter_dict["Feature_Id"] = feature_id
                        inter_dict["Feature_Name"] = feature_i
                        inter_list.append(inter_dict)
                    label_dict['SubMenu'] = inter_list
                    label_dict['Admin_Flag'] = 0
                    final_list.append(label_dict.copy())
    print(final_list)
    return Response(final_list)

# @api_view(['PUT'])
# def approvalsupdate(request, id):
#     # User_Email = request.data['User_Email']
#     feature = Approvals.objects.get(id=id)
#     serializer = ApprovalSerializer(instance=feature, data=request.data)
#     if serializer.is_valid():
#         serializer.save()
#         return Response(serializer.data, status=status.HTTP_200_OK)
#     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# @api_view(['PUT'])
# def approvalsupdate(request, id):
#     user_Email = request.data['User_Email']
#     mig_type = request.data['Migration_TypeId']
#     obj_type = request.data['Object_Type']
#     feature_name = request.data['Feature_Name']
#     access_type = request.data['Access_Type']
#     appr_status = request.data['Approval_Status']
#     feature = Approvals.objects.get(id=id)
#     serializer = ApprovalSerializer(instance=feature, data=request.data)
#     if serializer.is_valid():
#         serializer.save()
#         if appr_status == 'Approved' and access_type == 'ALL':
#             appr_data_view = Approvals.objects.filter(User_Email = user_Email,Migration_TypeId = mig_type,Object_Type = obj_type,Feature_Name = feature_name,Access_Type = 'View')
#             appr_data_edit = Approvals.objects.filter(User_Email = user_Email,Migration_TypeId = mig_type,Object_Type = obj_type,Feature_Name = feature_name,Access_Type = 'Edit')
#             appr_data_view.delete()
#             appr_data_edit.delete()
#         elif appr_status == 'Approved' and access_type == 'ALL' and obj_type == 'ALL':
#             appr_data_view = Approvals.objects.filter(User_Email = user_Email,Migration_TypeId = mig_type,Access_Type = 'View')
#             appr_data_edit = Approvals.objects.filter(User_Email = user_Email,Migration_TypeId = mig_type,Access_Type = 'Edit')
#             appr_data_view.delete()
#             appr_data_edit.delete()
#         elif appr_status == 'Approved' and access_type == 'ALL' and feature_name == 'ALL' and obj_type != 'ALL':
#             appr_data_view = Approvals.objects.filter(User_Email = user_Email,Migration_TypeId = mig_type,Object_Type = obj_type,Access_Type = 'View')
#             appr_data_edit = Approvals.objects.filter(User_Email = user_Email,Migration_TypeId = mig_type,Object_Type = obj_type,Access_Type = 'Edit')
#             appr_data_view.delete()
#             appr_data_edit.delete()
#         return Response(serializer.data, status=status.HTTP_200_OK)
#     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['PUT'])
def approvalsupdate(request, id):
    user_Email = request.data['User_Email']
    mig_type = request.data['Migration_TypeId']
    obj_type = request.data['Object_Type']
    feature_name = request.data['Feature_Name']
    access_type = request.data['Access_Type']
    appr_status = request.data['Approval_Status']
    feature = Approvals.objects.get(id=id)
    serializer = ApprovalSerializer(instance=feature, data=request.data)

    if appr_status == 'Approved':
        if obj_type != 'ALL' and feature_name =='ALL' and  access_type == 'View':
            app_data = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,Object_Type = obj_type,
                                                Access_Type='View').exclude(Feature_Name='ALL')
            if app_data:
                app_data.delete()
        elif obj_type != 'ALL' and feature_name =='ALL' and  access_type == 'Edit':
            app_data_view = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,Object_Type = obj_type,
                                                     Access_Type='View')
            if app_data_view:
                app_data_view.delete()
            app_data_edit = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,Object_Type = obj_type,
                                                     Access_Type='Edit').exclude(Feature_Name='ALL')
            if app_data_edit:
                app_data_edit.delete()
        elif obj_type != 'ALL' and feature_name =='ALL' and  access_type == 'ALL':
            app_data_view = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,Object_Type = obj_type,
                                                     Access_Type='View')
            if app_data_view:
                app_data_view.delete()
            app_data_edit = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,Object_Type = obj_type,
                                                     Access_Type='Edit')
            if app_data_edit:
                app_data_edit.delete()
            app_data_all = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,Object_Type = obj_type,
                                                    Access_Type='ALL').exclude(Feature_Name='ALL')
            if app_data_all:
                app_data_all.delete()
        elif obj_type == 'ALL' and access_type == 'View':
            app_data = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type, Access_Type='View').exclude(Object_Type = 'ALL',Feature_Name = 'ALL')
            if app_data:
                app_data.delete()
        elif obj_type == 'ALL' and access_type == 'Edit':
            app_data_view = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type, Access_Type='View')
            if app_data_view:
                app_data_view.delete()
            app_data_edit = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type, Access_Type='Edit').exclude(Object_Type = 'ALL',Feature_Name = 'ALL')
            if app_data_edit:
                app_data_edit.delete()
        elif obj_type == 'ALL' and access_type == 'ALL':
            app_data_view = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,Access_Type='View')
            if app_data_view:
                app_data_view.delete()
            app_data_edit = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,Access_Type='Edit')
            if app_data_edit:
                app_data_edit.delete()
            app_data_all = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,Access_Type='ALL').exclude(Object_Type = 'ALL',Feature_Name = 'ALL')
            if app_data_all:
                app_data_all.delete()
        elif access_type == 'Edit':
            app_data = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type, Object_Type=obj_type,
                                                Feature_Name=feature_name, Access_Type='View')
            if app_data:
                app_data.delete()
        elif access_type == 'ALL':
            app_data_view = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type, Object_Type=obj_type,
                                                     Feature_Name=feature_name, Access_Type='View')
            app_data_edit = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type, Object_Type=obj_type,
                                                     Feature_Name=feature_name, Access_Type='Edit')
            if app_data_view:
                app_data_view.delete()
            if app_data_edit:
                app_data_edit.delete()
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data, status=status.HTTP_200_OK)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


# @api_view(['GET','POST'])
# def admin_permissions(request):
#     email = request.data['email']
#     migtype =request.data['mig_type']
#     temp = Users.objects.filter(email=email)
#     temp = list(temp.values())
#     temp = temp[0]['admin_migrations']
#     if temp == None:
#         temp1 = []
#         temp = ''
#     else:
#         temp1 = temp.split(',')
#     if migtype not in temp1:
#         temp = temp + migtype + ','
#         a = Users.objects.get(email=email)
#         a.admin_migrations = temp
#         a.save()
#         return Response("Admin Access created the Migration")
#     else:
#         return Response("User Already Have Permission with Admin Access")


# @api_view(['GET','POST'])
# def admin_permissions(request):
#     email = request.data['email']
#     migtype =request.data['mig_type']
#     object_type = request.data['Object_Type']
#     user = Users.objects.get(email=email)
#     if user.admin_migrations!=None:
#         admin_access = user.admin_migrations.replace("\'", "\"")
#     else:
#         admin_access =''
#     if admin_access != '':
#         admin_access_dict = json.loads(admin_access)
#         if migtype not in admin_access_dict.keys():
#             object_list = []
#             object_list.append(object_type)
#             admin_access_dict[migtype] = object_list
#             a = Users.objects.get(email=email)
#             a.admin_migrations = admin_access_dict
#             a.save()
#             return Response("Admin Access created the Migration")
#         else:
#             if object_type not in admin_access_dict[migtype]:
#                 admin_access_dict[migtype].append(object_type)
#                 a = Users.objects.get(email=email)
#                 a.admin_migrations = admin_access_dict
#                 a.save()
#                 return Response("Admin Access created the Migration")
#             else:
#                 # print("else else")
#                 return Response("User Already Have Permission with Admin Access")
#     else:
#         final_dict = {}
#         object_list =[]
#         object_list.append(object_type)
#         final_dict[migtype] = object_list
#         a = Users.objects.get(email=email)
#         a.admin_migrations = final_dict
#         a.save()
#         return Response("Admin Access created the Migration")


@api_view(['GET','POST'])
def admin_permissions(request):
    email = request.data['email']
    migtype =request.data['mig_type']
    object_type = request.data['Object_Type']
    user = Users.objects.get(email=email)
    admin_access = user.admin_migrations.replace("\'", "\"")
    if admin_access != '':
        admin_access_dict = json.loads(admin_access)
        if migtype not in admin_access_dict.keys():
            object_list = []
            object_list.append(object_type)
            admin_access_dict[migtype] = object_list
            a = Users.objects.get(email=email)
            a.admin_migrations = admin_access_dict
            a.save()
            return Response("Admin access created for user")
        else:
            if object_type not in admin_access_dict[migtype]:
                if object_type == 'ALL':
                    admin_access_dict[migtype].clear()
                    admin_access_dict[migtype].append(object_type)
                    a = Users.objects.get(email=email)
                    a.admin_migrations = admin_access_dict
                    a.save()
                    return Response("Admin access created for user")
                else:
                    if 'ALL' not in admin_access_dict[migtype]:
                        admin_access_dict[migtype].append(object_type)
                        a = Users.objects.get(email=email)
                        a.admin_migrations = admin_access_dict
                        a.save()
                        return Response("Admin access created for user")
                    else:
                        return Response("User already has admin permission for ALL Object Types")

            else:
                return Response("User already has this admin permission")
    else:
        final_dict = {}
        object_list =[]
        object_list.append(object_type)
        final_dict[migtype] = object_list
        a = Users.objects.get(email=email)
        a.admin_migrations = final_dict
        a.save()
        return Response("Admin access created for user")

# @api_view(['GET','POST','PUT'])
# def remove_admin_permission(request):
#     User_Email = request.data['User_Email']
#     mig_type = request.data['Migration_Type']
#     user = Users.objects.get(email = User_Email)
#     admin_migration_list = user.admin_migrations.split(',')
#     admin_migration_list = [x for x in admin_migration_list if x != '']
#     if mig_type in admin_migration_list:
#         admin_migration_list.remove(mig_type)
#     if admin_migration_list:
#         admin_migartions = ','.join(admin_migration_list)
#         user.admin_migrations = admin_migartions + ','
#         user.save()
#         return Response("Admin access removed")
#     else:
#         user.admin_migrations = ''
#         user.save()
#         return Response("No Permissions")

@api_view(['GET', 'POST', 'PUT'])
def remove_admin_permission(request):
    User_Email = request.data['User_Email']
    mig_type = request.data['Migration_Type']
    object_type = request.data['Object_type']
    user = Users.objects.get(email=User_Email)
    admin_access = user.admin_migrations.replace("\'", "\"")
    if admin_access == '' or admin_access==None:
        user.admin_migrations = ''
        user.save()
        return Response("No Permissions")
    else:
        admin_access_dict = json.loads(admin_access)
        print(admin_access_dict)
        if mig_type in admin_access_dict.keys():
            if object_type == 'ALL':
                del admin_access_dict[mig_type]
                if len(admin_access_dict) == 0:
                    a = Users.objects.get(email=User_Email)
                    a.admin_migrations = ''
                    a.save()
                else:
                    a = Users.objects.get(email=User_Email)
                    a.admin_migrations = admin_access_dict
                    a.save()
            else:
                if object_type in admin_access_dict[mig_type]:
                    admin_access_dict[mig_type].remove(object_type)
                    if admin_access_dict[mig_type]:
                        a = Users.objects.get(email=User_Email)
                        a.admin_migrations = admin_access_dict
                        a.save()
                    else:
                        del admin_access_dict[mig_type]
                        if len(admin_access_dict) == 0:
                            a = Users.objects.get(email=User_Email)
                            a.admin_migrations = ''
                            a.save()
                        else:
                            a = Users.objects.get(email=User_Email)
                            a.admin_migrations = admin_access_dict
                            a.save()
        return Response("Admin access removed")

# @api_view(['GET','POST'])
# def admin_rm_migration_list(request):
#     final_list = []
#     inter_dict = {}
#     User_Email = request.data['User_Email']
#     user = Users.objects.get(email=User_Email)
#     admin_migration_list = user.admin_migrations.split(',')
#     admin_migration_list = [x for x in admin_migration_list if x != '']
#     for migration in admin_migration_list:
#         inter_dict['Migration_Type'] = migration
#         final_list.append(inter_dict.copy())
#     return Response(final_list)

# @api_view(['GET','POST'])
# def admin_rm_migration_list(request):
#     final_list = []
#     inter_dict = {}
#     User_Email = request.data['User_Email']
#     user = Users.objects.get(email=User_Email)
#     admin_access = user.admin_migrations
#     if admin_access != '':
#         admin_access = admin_access.replace("\'", "\"")
#         admin_access_dict = json.loads(admin_access)
#     else:
#         admin_access_dict = {}
#     migration_type_list = admin_access_dict.keys()
#
#     for migration in migration_type_list:
#         inter_dict['Migration_Type'] = migration
#         inter_dict['Object_types'] = admin_access_dict[migration]
#         final_list.append(inter_dict.copy())
#     return Response(final_list)


@api_view(['GET','POST'])
def admin_rm_migration_list(request):
    final_list = []
    inter_dict = {}
    User_Email = request.data['User_Email']
    user = Users.objects.get(email=User_Email)
    admin_access = user.admin_migrations
    if admin_access != '' and admin_access != None:
        admin_access = admin_access.replace("\'", "\"")
        admin_access_dict = json.loads(admin_access)
    else:
        admin_access_dict = {}
    for migration in admin_access_dict.keys():
        inter_dict['Migration_Type'] = migration
        final_list.append(inter_dict.copy())
    return Response(final_list)


@api_view(['GET','POST'])
def admin_rm_object_list(request):
    final_list = []
    inter_dict = {}
    User_Email = request.data['User_Email']
    mig_type = request.data['Migration_Type']

    user = Users.objects.get(email=User_Email)
    admin_access = user.admin_migrations
    if admin_access != '' and admin_access != None:
        admin_access = admin_access.replace("\'", "\"")
        admin_access_dict = json.loads(admin_access)
    else:
        admin_access_dict = {}
    if mig_type in admin_access_dict.keys():
        object_list = admin_access_dict[mig_type]
        for object_i in object_list:
            inter_dict['Object_type'] = object_i
            final_list.append(inter_dict.copy())
    return Response(final_list)


@api_view(['PUT'])
def permissionsupdate(request, User_Email):
    feature = Permissions.objects.get(User_Email=User_Email)
    serializer = PermissionSerializer(instance=feature, data=request.data)
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data, status=status.HTTP_200_OK)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


# @api_view(['GET','POST'])
# def permissionslist(request):
#     if len(request.data)>1:
#         User_Email = request.data['User_Email']
#         Migration_TypeId = request.data['Migration_TypeId']
#         features = Permissions.objects.filter(User_Email=User_Email, Migration_TypeId=Migration_TypeId)
#     else:
#         Migration_TypeId = request.data['Migration_TypeId']
#         features = Permissions.objects.filter(Migration_TypeId=Migration_TypeId)
#     serializer = PermissionSerializer(features, many=True)
#     return Response(serializer.data)


@api_view(['GET','POST'])
def permissionslist(request):
    if len(request.data)>1:
        Object_Type = request.data['Object_Type']
        User_Email = request.data['User_Email']
        Migration_TypeId = request.data['Migration_TypeId']
        if Object_Type == '' or Object_Type==None:
            features = Permissions.objects.filter(User_Email=User_Email, Migration_TypeId=Migration_TypeId)
        else:
            features = Permissions.objects.filter(User_Email=User_Email, Migration_TypeId=Migration_TypeId, Object_Type=Object_Type)
    else:
        Migration_TypeId = request.data['Migration_TypeId']
        features = Permissions.objects.filter(Migration_TypeId=Migration_TypeId)
    serializer = PermissionSerializer(features, many=True)
    return Response(serializer.data)
    
# @api_view(['GET'])
# def admin_users_list(request):
#     users = Users.objects.all()
#     admin_list =[]
#     admin_dict = {}
#     for user_i in users.values():
#         admin_mig_value = user_i['admin_migrations']
#         if admin_mig_value != None and admin_mig_value != '': #and user_i['is_superuser'] == False
#             admin_dict['Email'] =user_i['email']
#             # admin_mig_value = admin_mig_value#.replace('.',',')
#             admin_dict['Migration_Types'] = admin_mig_value
#             admin_list.append(admin_dict.copy())
#     return Response(admin_list)

@api_view(['GET'])
def admin_users_list(request):
    users = Users.objects.all()
    final_list =[]
    inter_dict = {}
    for user_i in users.values():
        admin_mig_value = user_i['admin_migrations']
        if admin_mig_value != None and admin_mig_value != '': #and user_i['is_superuser'] == False
            inter_dict['Email'] =user_i['email']
            admin_access = admin_mig_value.replace("\'", "\"")
            admin_access_dict = json.loads(admin_access)
            migration_type_list = admin_access_dict.keys()
            for migration in migration_type_list:
                inter_dict['Migration_Type'] = migration
                inter_dict['Object_types'] = admin_access_dict[migration]
                final_list.append(inter_dict.copy())
    return Response(final_list)

@api_view(['GET'])
def super_users_list(request):
    users = Users.objects.all()
    superuser_list =[]
    superuser_dict = {}
    for user_i in users.values():
        #admin_mig_value = user_i['admin_migrations']
        if user_i['is_superuser'] == True:
            superuser_dict['User_Name'] = user_i['username']
            superuser_dict['Email'] = user_i['email']
            superuser_list.append(superuser_dict.copy())
    return Response(superuser_list)


# @api_view(['POST'])
# def grant_access_approve(request):
#     User_Email = request.data['User_Email']
#     Object_Type = request.data['Object_Type']
#     Feature_Name = request.data['Feature_Name']
#     Access_Type = request.data['Access_Type']

#     appr_data = Approvals.objects.filter(User_Email=User_Email, Object_Type=Object_Type, Feature_Name=Feature_Name,
#                                 Access_Type=Access_Type)
#     if appr_data:
#         appr_status = appr_data.values()[0]['Approval_Status']
#         if appr_status == 'Approved':
#             serializer = ApprovalSerializer(appr_data,many=True)
#             return Response(serializer.data[0])
#         elif appr_status == 'Pending':
#             approval_record = Approvals.objects.get(User_Email=User_Email, Object_Type=Object_Type, Feature_Name=Feature_Name,
#                                 Access_Type=Access_Type)
#             serializer = ApprovalSerializer(instance=approval_record, data=request.data)
#             if serializer.is_valid():
#                 serializer.save()
#                 return Response(serializer.data, status=status.HTTP_200_OK)
#             return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
#     else:
#         serializer = ApprovalSerializer(data=request.data)
#         if serializer.is_valid():
#             serializer.save()
#             return Response(serializer.data, status=status.HTTP_201_CREATED)
#         return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# @api_view(['POST'])
# def grant_access_approve(request):
#     User_Email = request.data['User_Email']
#     mig_type = request.data['Migration_TypeId']
#     Object_Type = request.data['Object_Type']
#     Feature_Name = request.data['Feature_Name']
#     Access_Type = request.data['Access_Type']
#     request_appr_status = request.data['Approval_Status']
#
#     appr_data = Approvals.objects.filter(User_Email=User_Email,Migration_TypeId = mig_type,Object_Type=Object_Type, Feature_Name=Feature_Name,
#                                 Access_Type=Access_Type)
#     if appr_data:
#         appr_status = appr_data.values()[0]['Approval_Status']
#         if appr_status == 'Approved':
#             serializer = ApprovalSerializer(appr_data,many=True)
#             return Response(serializer.data[0])
#         elif appr_status == 'Pending':
#             approval_record = Approvals.objects.get(User_Email=User_Email, Object_Type=Object_Type, Feature_Name=Feature_Name,
#                                 Access_Type=Access_Type)
#             serializer = ApprovalSerializer(instance=approval_record, data=request.data)
#             if serializer.is_valid():
#                 serializer.save()
#                 if request_appr_status == 'Approved' and Access_Type == 'ALL':
#                     appr_data_view = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                               Object_Type=Object_Type, Feature_Name=Feature_Name,
#                                                               Access_Type='View')
#                     appr_data_edit = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                               Object_Type=Object_Type, Feature_Name=Feature_Name,
#                                                               Access_Type='Edit')
#                     appr_data_view.delete()
#                     appr_data_edit.delete()
#                 elif request_appr_status == 'Approved' and Access_Type == 'ALL' and Object_Type == 'ALL':
#                     appr_data_view = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                               Access_Type='View')
#                     appr_data_edit = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                               Access_Type='Edit')
#                     appr_data_view.delete()
#                     appr_data_edit.delete()
#                 elif request_appr_status == 'Approved' and Access_Type == 'ALL' and Feature_Name == 'ALL' and Object_Type != 'ALL':
#                     appr_data_view = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                               Object_Type=Object_Type, Access_Type='View')
#                     appr_data_edit = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                               Object_Type=Object_Type, Access_Type='Edit')
#                     appr_data_view.delete()
#                     appr_data_edit.delete()
#                 return Response(serializer.data, status=status.HTTP_200_OK)
#             return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
#     else:
#         serializer = ApprovalSerializer(data=request.data)
#         if serializer.is_valid():
#             serializer.save()
#             if request_appr_status == 'Approved' and Access_Type == 'ALL':
#                 appr_data_view = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                           Object_Type=Object_Type, Feature_Name=Feature_Name,
#                                                           Access_Type='View')
#                 appr_data_edit = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                           Object_Type=Object_Type, Feature_Name=Feature_Name,
#                                                           Access_Type='Edit')
#                 appr_data_view.delete()
#                 appr_data_edit.delete()
#             elif request_appr_status == 'Approved' and Access_Type == 'ALL' and Object_Type == 'ALL':
#                 appr_data_view = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                           Access_Type='View')
#                 appr_data_edit = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                           Access_Type='Edit')
#                 appr_data_view.delete()
#                 appr_data_edit.delete()
#             elif request_appr_status == 'Approved' and Access_Type == 'ALL' and Feature_Name == 'ALL' and Object_Type != 'ALL':
#                 appr_data_view = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                           Object_Type=Object_Type, Access_Type='View')
#                 appr_data_edit = Approvals.objects.filter(User_Email=User_Email, Migration_TypeId=mig_type,
#                                                           Object_Type=Object_Type, Access_Type='Edit')
#                 appr_data_view.delete()
#                 appr_data_edit.delete()
#             return Response(serializer.data, status=status.HTTP_201_CREATED)
#         return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['POST'])
def grant_access_approve(request):
    user_Email = request.data['User_Email']
    mig_type = request.data['Migration_TypeId']
    obj_type = request.data['Object_Type']
    feature_name = request.data['Feature_Name']
    access_type = request.data['Access_Type']
    appr_status = request.data['Approval_Status']
    if appr_status == 'Approved':
        if obj_type != 'ALL' and feature_name == 'ALL' and access_type == 'View':
            app_data = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type, Object_Type=obj_type,
                                                Access_Type='View').exclude(Feature_Name='ALL')
            if app_data:
                app_data.delete()
        elif obj_type != 'ALL' and feature_name == 'ALL' and access_type == 'Edit':
            app_data_view = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,
                                                     Access_Type='View')
            if app_data_view:
                app_data_view.delete()
            app_data_edit = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,
                                                     Access_Type='Edit').exclude(Feature_Name='ALL')
            if app_data_edit:
                app_data_edit.delete()
        elif obj_type != 'ALL' and feature_name == 'ALL' and access_type == 'ALL':
            app_data_view = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,
                                                     Access_Type='View')
            if app_data_view:
                app_data_view.delete()
            app_data_edit = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,
                                                     Access_Type='Edit')
            if app_data_edit:
                app_data_edit.delete()
            app_data_all = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                    Object_Type=obj_type,
                                                    Access_Type='ALL').exclude(Feature_Name='ALL')
            if app_data_all:
                app_data_all.delete()
        elif obj_type == 'ALL' and access_type == 'View':
            app_data = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                Access_Type='View').exclude(Object_Type='ALL', Feature_Name='ALL')
            if app_data:
                app_data.delete()
        elif obj_type == 'ALL' and access_type == 'Edit':
            app_data_view = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Access_Type='View')
            if app_data_view:
                app_data_view.delete()
            app_data_edit = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Access_Type='Edit').exclude(Object_Type='ALL', Feature_Name='ALL')
            if app_data_edit:
                app_data_edit.delete()
        elif obj_type == 'ALL' and access_type == 'ALL':
            app_data_view = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Access_Type='View')
            if app_data_view:
                app_data_view.delete()
            app_data_edit = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Access_Type='Edit')
            if app_data_edit:
                app_data_edit.delete()
            app_data_all = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                    Access_Type='ALL').exclude(Object_Type='ALL', Feature_Name='ALL')
            if app_data_all:
                app_data_all.delete()
        elif access_type == 'Edit':
            app_data = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type, Object_Type=obj_type,
                                                Feature_Name=feature_name, Access_Type='View')
            if app_data:
                app_data.delete()
        elif access_type == 'ALL':
            app_data_view = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,
                                                     Feature_Name=feature_name, Access_Type='View')
            app_data_edit = Approvals.objects.filter(User_Email=user_Email, Migration_TypeId=mig_type,
                                                     Object_Type=obj_type,
                                                     Feature_Name=feature_name, Access_Type='Edit')
            if app_data_view:
                app_data_view.delete()
            if app_data_edit:
                app_data_edit.delete()

    appr_data = Approvals.objects.filter(User_Email=user_Email,Migration_TypeId = mig_type,Object_Type=obj_type, Feature_Name=feature_name,
                                Access_Type=access_type)
    if appr_data:
        data_appr_status = appr_data.values()[0]['Approval_Status']
        if data_appr_status == 'Approved':
            serializer = ApprovalSerializer(appr_data,many=True)
            return Response(serializer.data[0])
        elif data_appr_status == 'Pending':
            approval_record = Approvals.objects.get(User_Email=user_Email,Migration_TypeId = mig_type, Object_Type=obj_type, Feature_Name=feature_name,
                                Access_Type=access_type)
            serializer = ApprovalSerializer(instance=approval_record, data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_200_OK)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    else:
        serializer = ApprovalSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)



@api_view(['POST'])
def createsuperadmin(request):
    email = request.data['email']
    features = Users.objects.get(email=email)
    features.is_superuser = True
    features.is_verified = True
    features.save()
    return Response('super admin created successfully')

@api_view(['POST'])
def removesuperadmin(request):
    email = request.data['email']
    features = Users.objects.get(email=email)
    features.is_superuser = False
    features.save()
    return Response('super admin removed successfully')

# @api_view(['GET'])
# def superadminlist(request):
#     features = Users.objects.filter(is_superuser=True)
#     serializer = Superuserlist(features, many=True)
#     return Response(serializer.data)


@api_view(['GET','POST'])
def migrationlistperuser(request):
    email = request.data['email']
    temp = Users.objects.filter(email=email)
    temp = list(temp.values())[0]
    temp = temp['admin_migrations']
    if temp:
        # print("entering")
        temp = temp.split(',')
        res = [ele for ele in temp if ele.strip()]
    else:
        print("not entering")
        res = []
    migrations_list = migrations.objects.values('Migration_TypeId')
    migrations_list= list(migrations_list.values('Migration_TypeId'))
    migrations_final_list = []
    for i in migrations_list:
        migrations_final_list.append(i['Migration_TypeId'])
    migrations_final_list = list(set(migrations_final_list))
    final_res = []
    for i in migrations_final_list:
        dict1= {}
        features = migrations.objects.filter(Migration_TypeId = i).distinct().order_by('Migration_TypeId')
        features = list(features.values())[0]
        ADMIN = 0
        if i in res:
            ADMIN = 1
        code = features['Code']
        dict1["title"] = i
        dict1["code"] = code
        dict1["admin"] = ADMIN
        final_res.append(dict1)
    return Response(final_res)