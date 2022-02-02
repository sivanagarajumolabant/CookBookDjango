import json
from .serializers import *
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import generics
# from reactdjango.cookbook.models import *
from rest_framework.decorators import api_view
from django.contrib.auth.models import User
from rest_framework.permissions import AllowAny
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser

import mimetypes

from reactdjango.settings import BASE_DIR, MEDIA_ROOT

from django.http import HttpResponse

import os
from importlib import import_module
import re
import sys


@api_view(['GET'])
def convert_python_code(request):
    # path_backend = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    path_backend = 'C:/projects/django/CookBook/backend'
    path_executable = path_backend + '/executable_modules/'
    # sys.path.append(0,path_executable)
    sys.path.insert(0,path_executable)
    source_code = """
    CREATE OR REPLACE PACKAGE SCHEMANAME.PACKAGENAME AS
    v1,
    v2,v3
    BEGIN
    procedure procedurename(v1)
    end;
    end;"""

    python_code = """def main(source_code):
                            print("Executing the SOurce Code:",source_code)
                            print("Testing Part1")
                            print("Testing Part2")
                            print("Testing Part3")
                            data = source_code
                            return data"""
    feature_name = "testdjango"
    python_code = re.sub(r'def\s+main','def '+feature_name,python_code)
    # substring = feature_name
    # python_code = python_code.replace('main', feature_name)
    # print('python_code is :', python_code)

    with open(path_executable+'/'+feature_name+'.py','w') as f:
        f.write(python_code)
    path_code_main = path_executable
    print('path_code_main : ',path_code_main)
    # os.
    # print(os.path)

    ax = import_module(feature_name)
    # print('attribute is : ', attribute)
    data = getattr(ax,feature_name)
    print('data is : ', data)
    executableoutput = data(source_code)
    # print('executableoutput is : ', executableoutput)
    dict1 = {'data': executableoutput}
    return Response(dict1)

# path_backend = 'C:/projects/django/CookBook/backend'
# path_executable = path_backend + '/executable_modules/'
# sys.path.append(path_executable)

@api_view(['POST'])
def convert_python_code1(request):
    # print(request.body)
    body_unicode = request.body.decode('utf-8')
    body_data = json.loads(body_unicode)
    feature_name = body_data['featurename']
    python_code = body_data['convcode']
    source_code = body_data['sourcecode']
    migration_typeid = body_data['migration_typeid']
    object_type = body_data['object_type']
    # print(python_code)
    # sys.path.insert(0, path_executable)
    path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    module_folder_name = "Modules"
    module_path = path+'/'+module_folder_name + '/' + migration_typeid + '/' + object_type + '/'
    sys.path.append(module_path)
    if not os.path.exists(module_path):
        os.makedirs(module_path)
    python_code = re.sub(r'def\s+main','def '+feature_name,python_code)
    # print(python_code)
    file_path =module_path+'/'+feature_name+'.py'
    sys.path.insert(0,file_path)
    python_code = python_code.replace("r@rawstringstart'",'')
    python_code = python_code.replace("'@rawstringend",'')
    # # print(python_code)
    # python_code = "".join([s for s in python_code.splitlines(True) if s])
    with open(file_path,'w') as f:
        f.write(python_code)
    path_code_main = file_path
    # print('path_code_main : ',path_code_main)
    # print("input ",source_code)
    # print("python code ", python_code )
    # os.chdir(path_code_main)
    # print(os.path)

    ax = import_module(feature_name)
    # print('attribute is : ', attribute)
    data = getattr(ax,feature_name)
    # print('data is : ', data)

    executableoutput = data(source_code)
    # print(executableoutput)
    dict1 = {'data': executableoutput}
    # print('executableoutput is : ', executableoutput)
    print(executableoutput)
    return Response(executableoutput)



class RegisterView(generics.CreateAPIView):
    queryset = User.objects.all()
    permission_classes = (AllowAny,)
    serializer_class = RegisterSerializer

class MyObtainTokenPairView(TokenObtainPairView):
    permission_classes = (AllowAny,)
    serializer_class = MyTokenObtainPairSerializer

@api_view(['GET'])
def migrationtypes(request):
    dict1 = {'oracle':1,'Mysql':2,'sqlserver':3}
    return Response(dict1)


@api_view(['GET'])
def fol(request,id):
    features1 = Feature.objects.filter(Object_Type='Procedure', Migration_TypeId=id)
    serializer1 = migrationlevelfeatures(features1,many=True)
    features1 = Feature.objects.filter(Object_Type='Function', Migration_TypeId=id)
    serializer2 = migrationlevelfeatures(features1,many=True)
    features1 = Feature.objects.filter(Object_Type='Package', Migration_TypeId=id)
    serializer3 = migrationlevelfeatures(features1,many=True)
    features1 = Feature.objects.filter(Object_Type='Index', Migration_TypeId=id)
    serializer4 = migrationlevelfeatures(features1,many=True)
    features1 = Feature.objects.filter(Object_Type='Materialized view', Migration_TypeId=id)
    serializer5 = migrationlevelfeatures(features1,many=True)
    features1 = Feature.objects.filter(Object_Type='Sequence', Migration_TypeId=id)
    serializer6 = migrationlevelfeatures(features1,many=True)
    features1 = Feature.objects.filter(Object_Type='Synonym', Migration_TypeId=id)
    serializer7 = migrationlevelfeatures(features1,many=True)
    features1 = Feature.objects.filter(Object_Type='Tabel', Migration_TypeId=id)
    serializer8 = migrationlevelfeatures(features1,many=True)
    features1 = Feature.objects.filter(Object_Type='Trigger', Migration_TypeId=id)
    serializer9 = migrationlevelfeatures(features1,many=True)
    features1 = Feature.objects.filter(Object_Type='Type', Migration_TypeId=id)
    serializer10 = migrationlevelfeatures(features1,many=True)
    features1 = Feature.objects.filter(Object_Type='View', Migration_TypeId=id)
    serializer11 = migrationlevelfeatures(features1,many=True)

    # return Response({'procedures':serializer1.data,'functions':serializer2.data,'Packages':serializer3.data})
    dict1 = {1: {'Label': 'Procedures', 'subMenu': serializer1.data},
             2: {'Label': 'Functions', 'subMenu': serializer2.data},
             3: {'Label': 'Packages', 'subMenu': serializer3.data},
             4: {'Label': 'Indexes', 'subMenu': serializer4.data},
             5: {'Label': 'Materialized views', 'subMenu': serializer5.data},
             6: {'Label': 'Sequences', 'subMenu': serializer6.data},
             7: {'Label': 'Synonyms', 'subMenu': serializer7.data},
             8: {'Label': 'Tabels', 'subMenu': serializer8.data},
             9: {'Label': 'Triggers', 'subMenu': serializer9.data},
             10: {'Label': 'Types', 'subMenu': serializer10.data},
             11: {'Label': 'Views', 'subMenu': serializer11.data}}
    return Response(dict1.values())

@api_view(['POST'])
# def sequence(request, Object_Type, Migration_TypeId):
def sequence(request):
    body_unicode = request.body.decode('utf-8')
    body_data = json.loads(body_unicode)
    print(body_data)
    Object_Type = body_data['Object_Type']
    Migration_TypeId = body_data['Migration_TypeId']
    features1 = Feature.objects.filter(Object_Type=Object_Type, Migration_TypeId=Migration_TypeId)
    serializer = SequenceSerializer(features1,many=True)
    dict1 = [serializer.data]
    return Response(dict1)

def download_file(request, file_name):
# def download_file(request):
    # fill these variables with real values
    fl_path = MEDIA_ROOT + '/media/'
    # fl_path = MEDIA_ROOT
    # filename = fl_path +'/COOKBOOK.docx'
    filename = fl_path +file_name
    # filename1 = 'test1.txt'
    filename1 = filename
    fl = open(filename, 'rb')
    mime_type, _ = mimetypes.guess_type(fl_path)
    response = HttpResponse(fl, content_type=mime_type)
    response['Content-Disposition'] = "attachment; filename=%s" % filename1
    return response

@api_view(['GET'])
def Featuredetail(request,id):
    features = Feature.objects.get(Feature_Id=id)
    serializer = FeatureSerializer(features)
    dict1 = [serializer.data]
    return Response(dict1)


class featureslist(generics.ListAPIView):
        queryset = Feature.objects.all()
        serializer_class= FeatureSerializer

class Featurecreate(generics.CreateAPIView):
        queryset = Feature.objects.all()
        serializer_class = commonSerializer

class Featuredelete(generics.RetrieveDestroyAPIView):
        queryset = Feature.objects.all()
        serializer_class = commonSerializer

class Featureupdate(generics.RetrieveUpdateAPIView):
        parser_classes = (MultiPartParser, FormParser, JSONParser)
        queryset = Feature.objects.all()
        serializer_class = commonSerializer
