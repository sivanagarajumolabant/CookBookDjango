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

from Features.settings import BASE_DIR, MEDIA_ROOT


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
    attachment = Attachments.objects.get(id=id)
    print(attachment)
    attachment.delete()
    fl_path = MEDIA_ROOT + '/media/' + '/' + migration_typeid + '/' + object_type + '/' + AttachmentType + '/'
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
    dictionary = {"Feature_Id": feature, 'AttachmentType': AttachmentType, "filename":filename,"Attachment":Attachment}
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
    fl_path = MEDIA_ROOT + '/media' + '/' + migration_typeid + '/' + object_type + '/' + attach_type + '/'
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
    try:
        body_unicode = request.body.decode('utf-8')
        body_data = json.loads(body_unicode)
        feature_name = body_data['featurename']
        python_code = body_data['convcode']
        source_code = body_data['sourcecode']
        migration_typeid = body_data['migration_typeid']
        object_type = body_data['object_type']
        path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        module_folder_name = "Modules"
        module_path = path + '/' + module_folder_name + '/' + migration_typeid + '/' + object_type + '/'
        sys.path.append(module_path)
        if not os.path.exists(module_path):
            os.makedirs(module_path)
        python_code = re.sub(r'def\s+main', 'def ' + feature_name, python_code)
        file_path = module_path + '/' + feature_name + '.py'
        sys.path.insert(0, file_path)
        python_code = python_code.replace("r@rawstringstart'", '')
        python_code = python_code.replace("'@rawstringend", '')
        with open(file_path, 'w') as f:
            f.write(python_code)
        module = import_module(feature_name)
        data = getattr(module, feature_name)
        executableoutput = data(source_code)
        # print(executableoutput)
        return Response(executableoutput, status=status.HTTP_200_OK)
    except Exception as err:
        print(err)
        return Response(err, status=status.HTTP_400_BAD_REQUEST)


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
        temp= {}
        temp['filename'] = x
        data1 = Attachments.objects.filter(Feature_Id=id, filename=x, AttachmentType='Sourcecode')

        a =list(data1.values_list())
        print(a)
        if len(a)== 0:
              temp['Sourcecode'] = 'N'
              # temp['sid'] = a[0]
        else:
             temp['Sourcecode'] = 'Y'
             temp['sid'] =  a[0][0]
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

        if temp['Sourcecode']=='Y' or temp['Expectedconversion']=='Y' or temp['Actualtargetcode']=='Y':
            result.append(temp)
    return Response(result)
