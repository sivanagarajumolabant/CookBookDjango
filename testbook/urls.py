from django.urls import path
from .views import *
# from . import views


from .views import MyObtainTokenPairView, RegisterView
from rest_framework_simplejwt.views import TokenRefreshView



urlpatterns = [
path('', featureslist.as_view(), name='featureslist'),
# path('function', function.as_view(), name='function'),
# path('procedure', procedure.as_view(), name='procedure'),
# path('package', package.as_view(), name='package'),
# path('function_ssql', function_ssql.as_view(), name='function_ssql'),
# path('procedure_ssql', procedure_ssql.as_view(), name='procedure_ssql'),
# path('package_ssql', package_ssql.as_view(), name='package_ssql'),
# path('function_mysql', function_mysql.as_view(), name='function_mysql'),
# path('procedure_mysql', procedure_ssql.as_view(), name='procedure_mysql'),
# path('package_mysql', package_ssql.as_view(), name='package_mysql'),
path('create', Featurecreate.as_view(), name='create'),
# path('create1/<int:pk>', Files_APIView_Detail.as_view(), name='create1'),
# path('create1', FeatureListView.as_view(), name='create1'),
# path('create2', ModelACreateAPIView.as_view(), name='create2'),
# path('create2', VehiclesListView.as_view(), name='create2'),
# path('detail/<int:pk>', Featuredetail.as_view(), name='create'),
path('detail/<int:id>', Featuredetail, name='detail'),
path('sequence', sequence, name='sequence'),
path('convert_python_code/' , convert_python_code, name='convert_python_code'),
path('convert_python_code1' , convert_python_code1, name='convert_python_code'),
path('delete/<int:pk>', Featuredelete.as_view(), name='delete'),
path('update/<int:pk>', Featureupdate.as_view(), name='update'),
path('downloads/<str:file_name>', download_file,name='downloads'),
# path('downloads/', download_file,name='downloads'),
# path('FU/<int:id>', views.FU, name='FU'),
# path('migrationlevelobjects', migrationlevelobjects1.as_view(), name='migrationlevelobjects'),
# path('featuresobjectlevel', views.featuresobjectlevel, name='featuresobjectlevel'),
path('fol/<int:id>', fol, name='fol'),
# path('migrationtype', migrationtype.as_view(), name='migrationtype'),
# path('login/', views.loginPage, name='login'),
# path('logout/', views.logoutpage, name='logout'),
# path('register/', views.registerPage, name='register'),
path('login/', MyObtainTokenPairView.as_view(), name='token_obtain_pair'),
path('login/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
path('register/', RegisterView.as_view(), name='auth_register'),
path('migrationtypes', migrationtypes,name='migrationtypes'),
# path('upload', upload_files.as_view(),name='upload'),
# path('multiplemodels', views.multiplemodels,name='multiplemodels'),
]
# if settings.DEBUG:
#     urlpatterns += static(settings.MEDIA_URL,
#                           document_root=settings.MEDIA_ROOT)

# if settings.DEBUG:
#     urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT) + \
#                    static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)