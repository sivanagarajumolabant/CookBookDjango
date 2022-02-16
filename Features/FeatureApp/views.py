from django.shortcuts import render
from rest_framework import viewsets, status
from .serializers import *
from .models import *
from rest_framework.decorators import api_view
from rest_framework.response import Response
import mimetypes
import json, os
from django.http import HttpResponse
from importlib import import_module
import re
import sys
# from emailcontent import email_verification_data

from Features.settings import BASE_DIR, MEDIA_ROOT

import jwt
from django.urls import reverse
from rest_framework import generics
from django.conf import settings
from django.contrib.sites.shortcuts import get_current_site
from rest_framework_simplejwt.tokens import RefreshToken
from .utils import Util


# Create your views here.
@api_view(['POST'])
def featurecreate(request):
    serializer = FeatureSerializer(data=request.data)
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET'])
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
    features = Feature.objects.filter(Object_Type=obj_type, Migration_TypeId=migtype)
    serializer = FeaturedropdownSerializer(features, many=True)
    return Response(serializer.data)


@api_view(['GET'])
def featuredetail(request, pk):
    features = Feature.objects.get(Feature_Id=pk)
    serializer = FeatureSerializer(features, many=False)
    return Response(serializer.data)


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
    features = Attachments.objects.filter(Feature_Id=id, AttachmentType='Sourcedescription')
    serializer = AttachementSerializer(features, many=True)
    return Response(serializer.data)


@api_view(['GET'])
def Targetdescription(request, id):
    features = Attachments.objects.filter(Feature_Id=id, AttachmentType='Targetdescription')
    serializer = AttachementSerializer(features, many=True)
    return Response(serializer.data)


@api_view(['GET'])
def Conversion(request, id):
    features = Attachments.objects.filter(Feature_Id=id, AttachmentType='Conversion')
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
    # path = 'C:/projects/CookBookDjango/CookBookDjango/Features'
    # body_unicode = request.body.decode('utf-8')
    # body_data = json.loads(body_unicode)
    # Feature_Id = body_data['Feature_Id']
    file_name = request.data['file_name']
    migration_typeid = request.data['migration_typeid']
    object_type = request.data['object_type']
    AttachmentType = request.data['AttachmentType']
    id = request.data['id']
    featurename = request.data['fname']
    attachment = Attachments.objects.get(id=id)
    # print(attachment)
    attachment.delete()
    fl_path = MEDIA_ROOT + '/media' + '/' + migration_typeid + '/' + object_type + '/' + featurename + '/' + AttachmentType + '/'
    # print(fl_path,'=========')
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
        return Response(attachements.data, status=status.HTTP_200_OK)
    return Response(attachements.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['DELETE'])
def featuredelete(request, pk):
    features = Feature.objects.get(Feature_Id=pk)
    features.delete()
    return Response('Deleted')


@api_view(['POST'])
# def sequence(request, Object_Type, Migration_TypeId):
def predessors(request):
    body_unicode = request.body.decode('utf-8')
    body_data = json.loads(body_unicode)
    Object_Type = body_data['Object_Type']
    Migration_TypeId = body_data['Migration_TypeId']
    features = Feature.objects.filter(Object_Type=Object_Type, Migration_TypeId=Migration_TypeId)
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
    fl_path = MEDIA_ROOT + '/media' + '/' + migration_typeid + '/' + object_type + '/' + featurename + '/' + attach_type + '/'
    filename = fl_path + file_name
    filename1 = filename
    fl = open(filename1, 'rb')
    # print(fl_path)
    mime_type, _ = mimetypes.guess_type(fl_path)
    response = HttpResponse(fl, content_type=mime_type)
    response['Content-Disposition'] = "attachment; filename=%s" % filename1
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
        module_path = path + '/' + module_folder_name + '/' + migration_typeid + '/' + object_type
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
        module = import_module(feature_name)
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
def miglevelobjects(request, id):
    objecttypes = ['Procedure', 'Function', 'Package', 'Index', 'Materialized view', 'Sequence', 'Synonym', 'Tabel',
                   'Trigger', 'Type', 'View']
    data_format_main = {}

    for index, i in enumerate(objecttypes):
        data_format = {}
        features = Feature.objects.filter(Object_Type=i, Migration_TypeId=id)
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
        data1 = Attachments.objects.filter(Feature_Id=id, filename=x, AttachmentType='Sourcecode')

        a = list(data1.values_list())
        print(a)
        if len(a) == 0:
            temp['Sourcecode'] = 'N'
            # temp['sid'] = a[0]
        else:
            temp['Sourcecode'] = 'Y'
            temp['sid'] = a[0][0]
        data1 = Attachments.objects.filter(Feature_Id=id, filename=x, AttachmentType='Actualtargetcode')
        print(data1)
        a = list(data1.values_list())
        print(a)
        if len(a) == 0:
            temp['Actualtargetcode'] = 'N'
            # temp['atid'] = a[0]
        else:
            temp['Actualtargetcode'] = 'Y'
            temp['atid'] = a[0][0]
        data1 = Attachments.objects.filter(Feature_Id=id, filename=x, AttachmentType='Expectedconversion')
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
        output_path = path + '/' + 'media' + '/' + migid + '/' + objtype + '/' + feature + '/' + 'Actualtargetcode' + '/'
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        module_path = path + '/' + 'media' + '/' + migid + '/' + objtype + '/' + feature + '/' + 'Conversion' + '/'
        sys.path.append(module_path)
        if not os.path.exists(module_path):
            return Response({"error": "Please upload Conversion Attachment before Converting into Files"},
                            status=status.HTTP_400_BAD_REQUEST)
        filter_files = Attachments.objects.filter(Feature_Id=feature_id, AttachmentType=attach_type)
        filter_values = list(filter_files.values_list())
        if filter_values:
            for file in filter_values:
                with open(file[4], 'r', encoding='utf-8') as f:
                    read_text = f.read()
                # print("check==========================", feature1)
                a = import_module(feature1)
                function_call = getattr(a, str(feature1).strip())
                output = function_call(read_text, 'hrpay')
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
            if Attachments.objects.filter(filename=row.filename, AttachmentType=row.AttachmentType).count() > 1:
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

        # absurl = 'http://localhost:3000/' + "?token=" + str(token)
        # email_body = 'Hi ' + user.username + ' Use below link to verify your account \n' + absurl
        # data = {'email_body': email_body, 'to_email': user.email,
        #         'email_subject': 'Verify your email'}
        # Util.send_email(data)
        return Response(user_data, status=status.HTTP_201_CREATED)


class VerifyEmail(generics.GenericAPIView):
    def get(self, request):
        token = request.GET.get('token')
        try:
            payload = jwt.decode(token, settings.SECRET_KEY)
            user = Users.objects.get(id=payload['user_id'])
            if not user.is_verified:
                user.is_verified = True
                user.is_active = True
                user.save()
            return Response({'email': 'Sucessfully Activated'}, status=status.HTTP_200_OK)
        except jwt.ExpiredSignatureError as identifier:
            return Response({'error': 'Activation Expired'}, status=status.HTTP_400_BAD_REQUEST)
        except jwt.exceptions.DecodeError as identifier:
            return Response({'error': 'Invalid token'}, status=status.HTTP_400_BAD_REQUEST)
