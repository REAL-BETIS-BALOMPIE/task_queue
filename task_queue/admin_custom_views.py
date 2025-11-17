import json

from celery import current_app as celery_app

from django.contrib import messages
from django.contrib.admin.options import IS_POPUP_VAR, TO_FIELD_VAR

from django.http import HttpResponseRedirect
from django.template.response import TemplateResponse
from django.urls import reverse
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from django.views.generic import FormView

from task_queue.constants import (
    OPERATION_TYPE_APPEND, OPERATION_TYPE_INSERT, OPERATION_TYPE_RUN_NOW,
    OPERATION_TYPE_RUN_STATE, OPERATION_TYPE_RUN_NEXT, QUEUE_TASK_ABORTED, QUEUE_TASK_ERROR,
    INTERNAL_PREFIX
)
from task_queue.helpers import get_celery_task
from task_queue.forms import (
    SelectTaskForm, ConfigureTaskForm, CreateTaskTemplateForm, SelectQueueForm, SelectQueuePriorityForm)
from task_queue.models import QueueTaskTemplate, QueueTaskGroup, QueueTask
from task_queue.permissions import SuperuserRequired
from task_queue import settings
from task_queue.tasks import BaseQueueTask
from task_queue.typing import OPERATION_TYPE


class AdminAddResponseView(SuperuserRequired, FormView):
    model = None
    popup_response_template = None
    result_object = None

    def __init__(self, *args, **kwargs):
        if self.model is None:
            raise ValueError('Model attibute must be set')
        self.result_object = None
        super().__init__(*args, **kwargs)

    def _get_result_object(self):
        if self.result_object is None:
            raise ValueError('result_object must be set in the class')
        return self.result_object

    def _response_post_save(self, request, obj):
        # COPIED FROM DJANGO: https://github.com/django/django/blob/master/django/contrib/admin/options.py
        opts = self.model._meta
        post_url = reverse('admin:%s_%s_changelist' %
                               (opts.app_label, opts.model_name))
        return HttpResponseRedirect(post_url)

    def response_add(self, request, obj, model_name, post_url_continue=None):
        # COPIED FROM DJANGO: https://github.com/django/django/blob/master/django/contrib/admin/options.py
        msg_dict = {
            'name': obj._meta.verbose_name,
            'obj': str(obj),
        }
        if IS_POPUP_VAR in request.POST:
            to_field = request.POST.get(TO_FIELD_VAR)
            if to_field:
                attr = str(to_field)
            else:
                attr = obj._meta.pk.attname
            value = obj.serializable_value(attr)
            popup_response_data = json.dumps({
                'value': str(value),
                'obj': str(obj),
            })
            return TemplateResponse(request, self.popup_response_template or [
                'admin/task_queue/%s/popup_response.html' % model_name,
                'admin/task_queue/popup_response.html',
                'admin/popup_response.html',
            ], {
                'popup_response_data': popup_response_data,
            })
        else:
            msg = format_html(
                _('The {name} “{obj}” was added successfully.'),
                **msg_dict
            )
            messages.add_message(request, messages.SUCCESS, msg)
            return self._response_post_save(request, obj)

    def get_form_kwargs(self):
        kw = super().get_form_kwargs()
        kw.update({
            'is_popup': self.request.GET.get(IS_POPUP_VAR),
            'to_field_var': self.request.GET.get(TO_FIELD_VAR)
        })

        task = self.request.GET.get(f'{INTERNAL_PREFIX}task')
        if task:
            kw['task_class'] = task
        return kw

    def form_valid(self, form):
        if isinstance(form, SelectTaskForm):
            return super().get(self, self.args, self.kwargs)
        super().form_valid(form)
        obj = self._get_result_object()
        return self.response_add(self.request, obj, self.model._meta.model_name)


class TaskConfigureBaseView(SuperuserRequired, FormView):
    create_form = None
    create_template = ''
    select_task_title = 'Select Task'
    select_task_subtitle = ''
    select_task_template = 'task_queue/admin/select_task.html'
    success_url = ''
    template_name = 'task_queue/admin/select_task.html'

    def get_task_description_extra_html(self, task):
        return ''

    def get_context_data(self, **kwargs):
        task = self.request.GET.get(f'{INTERNAL_PREFIX}task')
        task_descriptions = []
        task_description = None
        if task:
            task = get_celery_task(task)
            try:
                task_description = next((
                    (i, (
                        t.task_name, t.task_description, self.get_task_description_extra_html(t))
                     ) for i, t in celery_app.tasks.items()
                    if t == task
                ))
            except StopIteration:
                task_description = None
        else:
            task_descriptions = [
                (i, (
                    t.task_name, t.task_description, self.get_task_description_extra_html(t))
                 ) for i, t in celery_app.tasks.items()
                if isinstance(t, BaseQueueTask)
            ]
        context = super().get_context_data(**kwargs)
        return {
            **context,
            'select_task_title': self.select_task_title,
            'select_task_subtitle': self.select_task_subtitle,
            'task_name': task.task_name if task else '',
            'task_descriptions': task_descriptions,
            'task_description': task_description
        }

    def get_form_class(self):
        task = self.request.GET.get(f'{INTERNAL_PREFIX}task')
        if task:
            self.template_name = self.create_template
            return self.create_form
        else:
            self.template_name = self.select_task_template
            return SelectTaskForm

    def get_form_kwargs(self):
        kw = super().get_form_kwargs()

        task = self.request.GET.get(f'{INTERNAL_PREFIX}task')
        if task:
            kw.update({
                'task_class': task
            })
        return kw

    def form_invalid(self, form):
        return super().form_invalid(form)

    def form_valid(self, form):
        if isinstance(form, SelectTaskForm):
            return super().get(self, self.args, self.kwargs)
        self.on_form_valid(form)
        return super().form_valid(form)

    def on_form_valid(self, form):
        raise NotImplementedError('On form valid must be implemented')


class TaskLauncherView(TaskConfigureBaseView):
    create_form = ConfigureTaskForm
    create_template = 'task_queue/admin/configure_task.html'
    select_task_title = 'Launch Task'
    select_task_subtitle = 'Select a task to configure'
    success_url = settings.ADMIN_SUCCESS_URL

    def on_form_valid(self, form):
        form.create_task()


class CreateTaskTemplateView(AdminAddResponseView, TaskConfigureBaseView):
    create_form = CreateTaskTemplateForm
    create_template = 'task_queue/admin/create_task_template.html'
    model = QueueTaskTemplate
    select_task_title = 'Create Task Template'
    select_task_subtitle = 'Select a task to configure'
    success_url = settings.ADMIN_SUCCESS_URL

    def on_form_valid(self, form: CreateTaskTemplateForm):
        self.result_object = form.save()


class SelectQueueBaseView(FormView):
    can_insert = False
    form_class = SelectQueueForm
    select_queue_title = 'Select Queue'
    select_queue_subtitle = ''
    success_url = settings.ADMIN_SUCCESS_URL
    template_name = "task_queue/admin/select_queue.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return {
            **context,
            'select_queue_title': self.get_title(),
            'select_queue_subtitle': self.select_queue_subtitle,
            'can_insert': self.can_insert
        }

    def get_title(self):
        return self.select_queue_title

    def get_object(self):
        raise NotImplementedError('Get object must be implemented')

    def launch(self, operation: OPERATION_TYPE, obj, queue_id, form):
        raise NotImplementedError('Launch must be implemented')

    def form_valid(self, form):
        queue_id = form.cleaned_data[f'{INTERNAL_PREFIX}queue']
        operation = form.get_action()
        obj = self.get_object()

        self.launch(operation, obj, queue_id, form)
        return super().form_valid(form)


class LaunchTaskTemplateView(SuperuserRequired, SelectQueueBaseView):
    can_insert = True
    form_class = SelectQueuePriorityForm
    select_queue_subtitle = 'Select a Queue and an action'

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        obj = self.get_object()
        ct = obj.get_celery_task()
        kwargs['default_priority'] = getattr(ct, 'priority', 0)
        return kwargs

    def get_object(self):
        return QueueTaskTemplate.objects.get(id=self.kwargs.get('pk'))

    def launch(self, operation: OPERATION_TYPE, obj, queue_id, form):
        if operation == OPERATION_TYPE_INSERT and self.can_insert:
            obj.insert_at_queue(queue_id, priority=form.cleaned_data[f'{INTERNAL_PREFIX}priority'])
        elif operation == OPERATION_TYPE_APPEND:
            obj.append_to_queue(queue_id)
        elif operation == OPERATION_TYPE_RUN_NEXT:
            obj.run_next(queue_id)
        elif operation in [OPERATION_TYPE_RUN_STATE, OPERATION_TYPE_RUN_NOW]:
            obj.run_now(queue_id, wait_to_state=operation == OPERATION_TYPE_RUN_STATE)

    def get_title(self):
        obj = self.get_object()
        return f'Launch Task: {obj}'


class LaunchTaskGroupView(SuperuserRequired, SelectQueueBaseView):
    can_insert = True
    form_class = SelectQueuePriorityForm
    select_queue_subtitle = 'Select a Queue and an action'

    def get_object(self):
        return QueueTaskGroup.objects.get(id=self.kwargs.get('pk'))

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['default_priority'] = self.get_object().priority
        return kwargs

    def launch(self, operation: OPERATION_TYPE, obj, queue_id, form):
        obj.perform_operation(queue_id, operation, form.cleaned_data.get('priority'))

    def get_title(self):
        obj = self.get_object()
        return f'Launch Task Group: {obj}'


class LaunchTaskAgainView(LaunchTaskTemplateView):
    can_insert = True
    form_class = SelectQueuePriorityForm
    select_queue_subtitle = 'Select a Queue and an action'

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['default_priority'] = self.get_object().priority
        try:
            kwargs['default_queue'] = getattr(self.get_object().queue, 'id', None)
        except QueueTask.RelatedObjectDoesNotExist:
            pass
        return kwargs

    def get_object(self):
        return QueueTask.objects.filter(
            process_status__in=[QUEUE_TASK_ABORTED, QUEUE_TASK_ERROR]
        ).get(id=self.kwargs.get('pk'))

    def launch(self, operation: OPERATION_TYPE, obj: QueueTask, queue_id, form):
        ct = obj.get_celery_task()
        if operation == OPERATION_TYPE_INSERT and self.can_insert:
            ct.insert_at_queue(
                *obj.task_args, queue_id=queue_id, priority=form.cleaned_data[f'{INTERNAL_PREFIX}priority'], existing_id=str(obj.id),
                **obj.task_kwargs
            )
        elif operation == OPERATION_TYPE_APPEND:
            ct.append_to_queue(*obj.task_args, queue_id=queue_id, existing_id=str(obj.id), **obj.task_kwargs)
        elif operation == OPERATION_TYPE_RUN_NEXT:
            ct.run_next(*obj.task_args, queue_id=queue_id, existing_id=str(obj.id), **obj.task_kwargs)
        elif operation in [OPERATION_TYPE_RUN_STATE, OPERATION_TYPE_RUN_NOW]:
            ct.run_now(
                *obj.task_args, queue_id=queue_id, wait_to_state=operation == OPERATION_TYPE_RUN_STATE,
                existing_id=str(obj.id), **obj.task_kwargs
            )
        obj.queues_as_aborted.set([])
        obj.queues_as_failed.set([])

    def get_title(self):
        obj = self.get_object()
        return f'Re-Launch Task: {obj}'
