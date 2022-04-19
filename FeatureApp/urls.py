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
                  path('fversions/', views.feature_version_list),
                  path('flist', views.featurelist),
                  path('fdetail/<str:feature_name>', views.featuredetail),
                  path('fdetailcatlog/', views.feature_catalog_access_check),
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
                  # path('testing/', views.add_view),
                  path('fvlist/', views.featurelistperuser),

                  path('migrationsscreate/', views.migrationsscreate, name='migrationsscreate'),
                  path('migrationviewlist/', views.migrationviewlist, name='migrationviewlist'),
                  path('objectviewtlist/', views.objectviewtlist, name='objectviewtlist'),

                  path('approvalscreate', views.approvalscreate),
                  path('approvalslist', views.approvalslist),
                  path('userslist/', views.userslist, name='userslist'),
                  path('permissionscreate/', views.permissionscreate, name='permissionscreate'),

                  path('approvalsupdate/<int:id>', views.approvalsupdate, name='approvalsupdate'),
                  path('permissionslist/', views.permissionslist, name='permissionslist'),
                  # path('sourcecode/<int:id>',views.Sourcecode),
                  # path('atargetcode/<int:id>',views.Actualtargetcode),
                  # path('etargetcode/<int:id>',views.Expectedconversion),
                  path('adminpermission/', views.admin_permissions, name='adminpermission'),

                  path('removeadminmigrations/', views.remove_admin_permission),
                  path('adminrmmigrationlist/', admin_rm_migration_list),
                  path('adminsobjectslist/',admin_rm_object_list),

                # header api's
                  path('createflagcheck/', create_check_list, name='create_check_list'),
                  path('migrationlistperuser/', migrationlistperuser, name='migrationlistperuser'), # for header mig types list
                  path('usersfeaturelist/', views.migration_user_view, name='usersfeaturelist'),

                  path('adminlist/', views.admin_users_list, name='admin_users_list'),
                  path('superuserlist/', views.super_users_list, name='super_users_list'),
                  path('grantaccess/', views.grant_access_approve, name='grant_access_approve'),
                  path('createsuperadmin/', views.createsuperadmin, name='createsuperadmin'),
                  path('removesuperadmin/', views.removesuperadmin, name='removesuperadmin'),
                  path('objectadminviewtlist/', views.objectadminviewtlist, name='objectadminviewtlist'),
                  path('templatedownload/', views.template_download),
                  path('pdfdownload/', views.pdf_download),
                  path('featureapprovalslist/', views.feature_approval_list),
                  path('featureapprovalcreate/', views.approval_featurecreate),
                  path('project_versions_list/', views.project_versions_list, name='project_versions_list'),
                  path('create_project_version/', views.create_project_version, name='create_project_version'),
                  path('createuseradmin/', create_user_admin),
                  path('user_admin_permissions/', user_admin_permissions),
                  path('useradminlist/', useradminlist),
                  path('removeuseradmin/', removesuperadmin),
                  path('userwaiting_list/', user_waiting_list),
                  path('userslist_useradmin/', userslist_useradminpage),
                  path('migtypes_useradmin/', migrationlist_useradmin),
                  path('useradminactions/', user_admin_actions)

              ] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
