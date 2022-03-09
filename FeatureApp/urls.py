from django.urls import path, include
from . import views
from rest_framework import routers
from django.conf.urls.static import static
from django.conf import settings
from .views import *

urlpatterns = [
                  path('fcreate', views.featurecreate),
                  path('fdelete/<int:pk>', views.featuredelete),
                  path('fupdate/<int:pk>', views.featureupdate),
                  path('flist', views.featurelist),
                  path('fdetail/<int:pk>', views.featuredetail),
                  path('attachmentsupdate/<int:pk>', views.Attcahmentupdate),
                  # path('template/<str:file_name>', views.download_file),
                  path('download_att', views.download_attachment),
                  path('predessors', views.predessors),
                  path('autoconv', views.conversion),
                  path('miglevelobjects/<migtypeid>', views.miglevelobjects),
                  path('fnlist', views.featuredropdownlist),
                  path('attlist', views.att_list),
                  path('sourcedesc/<int:id>', views.Sourcedescription),
                  path('targetdesc/<int:id>', views.Targetdescription),
                  path('convatt/<int:id>', views.Conversion),
                  path('codefiles/<int:id>', views.attachentsqlcodefiles),
                  path('attdelete', views.attachment_delete),
                  path('convertfiles', views.feature_conversion_files),
                  path('email-verify/', VerifyEmail.as_view(), name="email-verify"),

                  path('password-reset/<uidb64>/<token>/', PasswordTokenCheckAPI.as_view(),
                       name='password-reset-confirm'),
                  path('request-reset-email/', RequestPasswordResetEmail.as_view(), name='request-reset-email'),
                  path('password-reset-complete/', SetNewPasswordAPIView.as_view(), name='password-reset-complete'),
                  path('register/', RegisterView.as_view(), name='auth_register'),
                  path('tablesdata/', views.create_tablepage_featuresdata),
                  path('requestfndata/', views.get_Featurenames),
                  path('resendemail/', ResendVerifyEmail.as_view()),
                  path('testing/', views.add_view),
                  path('fvlist/', views.featurelistperuser),

                  path('migrationsscreate/', views.migrationsscreate, name='migrationsscreate'),
                  path('migrationviewlist/', views.migrationviewlist, name='migrationviewlist'),
                  path('objectviewtlist/<Migration_TypeId>', views.objectviewtlist, name='objectviewtlist'),

                  path('approvalscreate', views.approvalscreate),
                  path('approvalslist', views.approvalslist),
                  path('userslist/', userslist, name='userslist'),
                  path('permissionscreate/', permissionscreate, name='permissionscreate'),
                  path('usersfeaturelist/', migration_user_view, name='usersfeaturelist'),
                  path('approvalsupdate/<User_Email>', approvalsupdate, name='approvalsupdate')
                  # path('sourcecode/<int:id>',views.Sourcecode),
                  # path('atargetcode/<int:id>',views.Actualtargetcode),
                  # path('etargetcode/<int:id>',views.Expectedconversion),

              ] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
