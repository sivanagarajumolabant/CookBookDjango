from django.shortcuts import render
from rest_framework import viewsets, status
from .serializers import *
from config.config import frontend_url
from .models import *
from rest_framework.decorators import api_view
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
import mimetypes
import json, os
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
    file_name = request.data['file_name']
    migration_typeid = request.data['migration_typeid']
    object_type = request.data['object_type']
    AttachmentType = request.data['AttachmentType']
    id = request.data['id']
    featurename = request.data['fname']
    attachment = Attachments.objects.get(id=id)
    attachment.delete()
    fl_path = MEDIA_ROOT + '/media' + '/' + migration_typeid + '/' + object_type + '/' + featurename + '/' + AttachmentType + '/'
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
    fid = body_data['feature_id']
    filter_files = Attachments.objects.filter(Feature_Id=fid, AttachmentType=attach_type, filename=file_name)
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
        features = Feature.objects.filter(Object_Type=i, Migration_TypeId=migtypeid)
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
        module_file = os.listdir(module_path)[0]
        # file_path = module_path + '/' + feature1 + '.py'
        file_path = module_path + '/' + module_file
        sys.path.insert(0, file_path)
        filter_files = Attachments.objects.filter(Feature_Id=feature_id, AttachmentType=attach_type)
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
        mail.send_mail(subject, plain_message, from_email, [to], html_message=html_message)
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
            mail.send_mail(subject, plain_message, from_email, [to], html_message=html_message)
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

            absurl = frontend_url+'resetpassword?token=' + str(token) + "?uid=" + uidb64
            # email_body = 'Hello,  \n Use below link to reset your password \n' + absurl
            # data = {'email_body': email_body, 'to_email': user.email,
            #         'email_subject': 'Reset your password'}
            # Util.send_email(data)

            subject = 'Forgot Password'
            html_message = render_to_string('forgotpassword.html', {'url': absurl})
            plain_message = strip_tags(html_message)
            from_email = EMAIL_HOST_USER
            to = user.email
            mail.send_mail(subject, plain_message, from_email, [to], html_message=html_message)
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
            payload = jwt.decode(token, settings.SECRET_KEY)
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
    dict1 = {'Procedures': 0, 'Function': 1, 'Package': 2, 'Index': 3, 'Materialized view': 4}
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
    data = Feature.objects.filter(Migration_TypeId=Migration_TypeId, Object_Type=Object_Type)
    serializer = FeatureSerializer(data, many=True)
    return Response(serializer.data)


@api_view(['POST'])
def get_Featurenames(request):
    # body_unicode = request.body.decode('utf-8')
    # body_data = json.loads(body_unicode)
    Migration_TypeId = request.data['Migration_TypeId']
    Object_Type = request.data['Object_Type']
    if str(Object_Type).upper() == 'ALL':
        features = Feature.objects.all()
    else:
        features = Feature.objects.filter(Migration_TypeId=Migration_TypeId, Object_Type=Object_Type)
    serializer = migrationlevelfeatures(features, many=True)
    return Response(serializer.data, status=status.HTTP_200_OK)


@api_view(['POST'])
def migrationsscreate(request):
    migration_type = request.data['Migration_TypeId']
    serializer = migrationcreateserializer(data=request.data)
    if serializer.is_valid():
        serializer.save(Code = migration_type.replace(' ','_'))
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

@api_view(['GET'])
def migrationviewlist(request):
    features = migrations.objects.values('Migration_TypeId','Code').distinct()
    serializer = migrationviewserializer(features, many=True)
    print(serializer.data[0])
    return Response(serializer.data)


@api_view(['GET'])
def objectviewtlist(request, Migration_TypeId):
    Migration_TypeId  =str(Migration_TypeId).strip()
    print(Migration_TypeId)
    # features = migrations.objects.all()
    features = migrations.objects.filter(Object_Type__isnull=False, Migration_TypeId=Migration_TypeId)
    serializer = objectviewserializer(features, many=True)
    return Response(serializer.data)


# @api_view(['POST'])
# def approvalscreate(request):
#     serializer = ApprovalSerializer(data=request.data)
#     if serializer.is_valid():
#         serializer.save()
#         return Response(serializer.data, status=status.HTTP_201_CREATED)
#     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

@api_view(['POST'])
def approvalscreate(request):
    # print(request.data)
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


@api_view(['GET'])
def approvalslist(request):
    features = Approvals.objects.all()
    serializer = ApprovalSerializer(features, many=True)
    return Response(serializer.data)


@api_view(['GET'])
def userslist(request):
    features = Users.objects.all()
    serializer = usersserializer(features, many=True)
    return Response(serializer.data)