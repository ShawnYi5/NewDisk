"""disk_snapshot_service URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url

from task_manager import views as task_manager_views

# 接口原则
#   提供给业务系统的接口都是用 POST 类型，接口按照操作的业务对象分组聚合在一个 VIEW 中

urlpatterns = [
    url(r'^task/host_snapshot/$', task_manager_views.HostSnapshotView.as_view(), name='host_snapshot'),


]
