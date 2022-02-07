from django.urls import path, include
from . import views
from rest_framework import routers
from django.conf.urls.static import static
from django.conf import settings



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
    path('miglevelobjects/<int:id>', views.miglevelobjects),
    path('fnlist',views.featuredropdownlist),
    path('attlist',views.att_list),
    path('sourceattlist',views.source_atta_detail),
    path('targetattlist',views.target_atta_detail),
    path('convattlist',views.conv_atta_detail),
    path('attdelete',views.attachment_delete),

]+static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
