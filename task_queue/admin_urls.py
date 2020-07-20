from django.urls import path
from task_queue.admin_custom_views import (
    TaskLauncherView, CreateTaskTemplateView, LaunchTaskTemplateView, LaunchTaskGroupView,
    LaunchTaskAgainView
)

urlpatterns = [
    path('queuetask/select_task/', TaskLauncherView.as_view(), name='tasks_queue_queuetask_select'),
    path('queuetasktemplate/add/', CreateTaskTemplateView.as_view(), name='task_queue_queuetasktemplate_add'),
    path('queuetasktemplate/<pk>/launch/', LaunchTaskTemplateView.as_view(), name='task_queue_queuetasktemplate_launch'),
    path('queuetaskgroup/<pk>/launch/', LaunchTaskGroupView.as_view(), name='task_queue_queuetaskgroup_launch'),
    path('queuetask/<pk>/launch/', LaunchTaskAgainView.as_view(), name='task_queue_queuetask_launch')
]
