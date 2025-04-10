from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('trigger-processing/', views.trigger_pdf_processing, name='trigger_pdf_processing'),

]
