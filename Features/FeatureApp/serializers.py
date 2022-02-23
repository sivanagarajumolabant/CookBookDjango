from rest_framework import serializers
from .models import *
# from django.contrib.auth.models import User
from rest_framework.validators import UniqueValidator
from django.contrib.auth.password_validation import validate_password
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer


class FeaturedropdownSerializer(serializers.ModelSerializer):
    class Meta:
        model = Feature
        fields = ('Feature_Name',)


class FeatureSerializer(serializers.ModelSerializer):
    class Meta:
        model = Feature
        fields = "__all__"


class AttachementSerializer(serializers.ModelSerializer):
    # Attachment = serializers.FileField()
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


class ConversionfilesSerializer(serializers.ModelSerializer):
    class Meta:
        model = Attachments
        fields = ('Attachment',)


class RegisterSerializer(serializers.ModelSerializer):
    email = serializers.EmailField(
        required=True,
        validators=[UniqueValidator(queryset=Users.objects.all())]
    )

    password = serializers.CharField(write_only=True, required=True, validators=[validate_password])
    password2 = serializers.CharField(write_only=True, required=True)

    class Meta:
        model = Users
        fields = ('username', 'password', 'password2', 'email', 'first_name', 'last_name')
        extra_kwargs = {
            'first_name': {'required': True},
            'last_name': {'required': True},
            'email': {'required': True},
        }

    def validate(self, attrs):
        if attrs['password'] != attrs['password2']:
            raise serializers.ValidationError({"password": "Password fields didn't match."})

        return attrs

    def create(self, request):
        user = Users.objects.create(
            username=self.validated_data['username'],
            email=self.validated_data['email'],
            first_name=self.validated_data['first_name'],
            last_name=self.validated_data['last_name']
        )

        user.set_password(self.validated_data['password'])
        user.is_active = False
        user.save()
        # token = RefreshToken.for_user(user).access_token
        # relative_link = reverse('email-verify')
        # current_site = get_current_site(request)
        # print(current_site,"currentsite")
        # subject = 'welcome to GFG world'
        # current_site = get_current_site(request=request)
        # mail_subject = 'Activation link has been sent to your email id'
        # message = {
        #     'user': user,
        #     # 'domain': current_site.domain,
        #     'uid': urlsafe_base64_encode(force_bytes(user.pk)),
        #     'token': account_activation_token.make_token(user),
        # }
        # to_email = [user.email, ]
        # email = EmailMessage(mail_subject, message, to=[to_email])
        # email.send()
        return user

class viewlevelfeatures(serializers.ModelSerializer):
    class Meta:
        model = Users
        fields = ('can_view',)


class MyTokenObtainPairSerializer(TokenObtainPairSerializer):

    # @classmethod
    # def get_token(cls, user):
    #     token = super(MyTokenObtainPairSerializer, cls).get_token(user)
    #
    #     # admin_role =0
    #     # if user.is_admin:
    #     #     print("entering")
    #     #     admin_role =1
    #     # print(admin_role,"admin_role")
    #     # Add custom claims
    #     token['username'] = user.username
    #     return token
    def validate(self, attrs):
        data = super().validate(attrs)
        refresh = self.get_token(self.user)
        data['refresh'] = str(refresh)
        data['access'] = str(refresh.access_token)

        # Add extra responses here
        # data['username'] = self.user.username
        data['admin'] = self.user.is_superuser
        # data['groups'] = self.user.groups.values_list('name', flat=True)
        return data
