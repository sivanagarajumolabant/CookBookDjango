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


@api_view(['GET'])
def featuredetail(request, pk):
    features = Feature.objects.get(Feature_Id=pk)
    serializer = FeatureSerializer(features, many=False)
    return Response(serializer.data)


@api_view(['POST'])
def featureupdate(request, pk):
    feature = Feature.objects.get(Feature_Id=pk)
    serializer = FeatureSerializer(instance=feature, data=request.data)
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data, status=status.HTTP_200_OK)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['POST'])
def Attcahmentupdate(request, pk):
    feature = Feature.objects.get(Feature_Id=pk)
    AttachmentType = request.data['AttachmentType']
    Attachment = request.FILES.get('Attachment')
    dictionary = {"Feature_Id": feature, 'AttachmentType': AttachmentType, 'Attachment': Attachment}
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


@api_view(['GET'])
def download_file(request, file_name):
    fl_path = MEDIA_ROOT + '/media/'
    filename = fl_path + file_name
    filename1 = filename
    fl = open(filename, 'rb')
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
        data_format_main[index+1] = data_format
    datavalues = data_format_main.values()
    return Response(datavalues, status=status.HTTP_200_OK)
