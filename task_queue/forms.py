from django import forms
from django.contrib.admin.options import IS_POPUP_VAR, TO_FIELD_VAR

from celery import current_app as celery_app

from task_queue.constants import (
    OPERATION_TYPE_PREFIX, OPERATION_TYPE_APPEND, OPERATION_TYPE_INSERT, OPERATION_TYPE_RUN_NOW,
    OPERATION_TYPE_RUN_STATE, OPERATION_TYPE_RUN_NEXT, INTERNAL_PREFIX)
from task_queue.helpers import get_default_queue, get_parameters
from task_queue.models import TaskQueue, QueueTaskTemplate
from task_queue.tasks import BaseQueueTask
from task_queue.typing import OPERATION_TYPE

INTERNAL_FIELD_ORDER = [f'{INTERNAL_PREFIX}task', f'{INTERNAL_PREFIX}queue', f'{INTERNAL_PREFIX}priority']


class PopupForm(forms.Form):

    def __init__(self, is_popup=False, to_field_var=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if is_popup:
            self.fields[IS_POPUP_VAR] = forms.IntegerField(initial=1, widget=forms.HiddenInput)
            self.fields[TO_FIELD_VAR] = forms.CharField(initial=to_field_var, widget=forms.HiddenInput)


class SelectTaskForm(PopupForm):
    required_css_class = 'required'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        tasks = list(map(lambda x: (x[0], x[1].task_name), filter(
            lambda y: isinstance(y[1], BaseQueueTask),
            celery_app.tasks.items()
        )))
        tasks.sort(key=lambda x: x[1])
        self.fields[f'{INTERNAL_PREFIX}task'] = forms.ChoiceField(label='Task', choices=tasks, required=True)


class BaseTaskForm(PopupForm):
    required_css_class = 'required'
    task_func = None

    def __init__(self, task_class, *args, **kwargs):
        task = celery_app.tasks.get(task_class)
        self.task_func = task
        super().__init__(*args, **kwargs)
        extra_fields = task.get_all_form_fields()
        self.fields[f'{INTERNAL_PREFIX}task'] = forms.CharField(
            label='Task', widget=forms.HiddenInput(), initial=task_class)
        self.fields.update(extra_fields)

    def get_task_args_kwargs(self, params):
        cleaned = self.cleaned_data
        if not cleaned:
            raise ValueError('is_valid must be called before this')

        cleaned_keys = cleaned.keys()
        no_internal_cleaned = {}
        no_internal_cleaned_keys = []
        for k in cleaned_keys:
            if not k.startswith(INTERNAL_PREFIX):
                no_internal_cleaned[k] = cleaned[k]
                no_internal_cleaned_keys.append(k)

        args = []

        for name, param in params:
            if name in no_internal_cleaned_keys:
                is_kwarg = param.default is not param.empty
                if not is_kwarg:
                    args.append(no_internal_cleaned.pop(name))
        return args, no_internal_cleaned

    def clean(self):
        cleaned_data = super().clean()
        cleaned_data = self.task_func.clean_form(cleaned_data)
        if cleaned_data is None:
            raise ValueError('clean_form must always return the cleaned data')
        return cleaned_data


class ConfigureTaskForm(BaseTaskForm):

    def __init__(self, task_class, *args, **kwargs):
        super().__init__(task_class, *args, **kwargs)
        self.fields[f'{INTERNAL_PREFIX}queue'] = forms.ChoiceField(
            label='Queue',
            choices=TaskQueue.objects.values_list('id', 'name'),
            initial=str(get_default_queue().id)
        )
        self.fields[f'{INTERNAL_PREFIX}priority'] = forms.IntegerField(
            label='Priority',
            initial=self.task_func.default_priority)
        self.order_fields(INTERNAL_FIELD_ORDER)

    def create_task(self):
        cleaned = self.cleaned_data
        queue_id = cleaned.get(f'{INTERNAL_PREFIX}queue')
        priority = cleaned.get(f'{INTERNAL_PREFIX}priority')
        task = celery_app.tasks.get(cleaned.get(f'{INTERNAL_PREFIX}task'))
        params = get_parameters(task.handle)
        operation: OPERATION_TYPE = next(key for key in self.data.keys() if key.startswith(OPERATION_TYPE_PREFIX))
        args, kwargs = self.get_task_args_kwargs(params)

        if operation == OPERATION_TYPE_INSERT:
            task.insert_at_queue(*args, queue_id=queue_id, priority=priority, **kwargs)
        elif operation == OPERATION_TYPE_APPEND:
            task.append_to_queue(*args, queue_id=queue_id, **kwargs)
        elif operation == OPERATION_TYPE_RUN_NEXT:
            task.run_next(*args, queue_id=queue_id, **kwargs)
        elif operation in [OPERATION_TYPE_RUN_STATE, OPERATION_TYPE_RUN_NOW]:
            task.run_now(*args, queue_id=queue_id, wait_to_state=operation == OPERATION_TYPE_RUN_STATE, **kwargs)


class CreateTaskTemplateForm(BaseTaskForm):

    def save(self, commit=True):
        cleaned = self.cleaned_data
        task_class = cleaned.get(f'{INTERNAL_PREFIX}task')
        task = celery_app.tasks.get(task_class)
        params = get_parameters(task.handle)
        args, kwargs = self.get_task_args_kwargs(params)
        instance = QueueTaskTemplate(
            task_name=task.task_name,
            task_class=task_class,
            task_args=args,
            task_kwargs=kwargs
        )
        if commit:
            instance.save()
        return instance


class SelectQueueForm(forms.Form):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields[f'{INTERNAL_PREFIX}queue'] = forms.ChoiceField(
            label='Queue',
            choices=TaskQueue.objects.values_list('id', 'name'),
            initial=str(get_default_queue().id)
        )

    def get_action(self):
        return next(key for key in self.data.keys() if key.startswith(OPERATION_TYPE_PREFIX))


class SelectQueuePriorityForm(SelectQueueForm):

    def __init__(self, *args, default_priority=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields[f'{INTERNAL_PREFIX}priority'] = forms.IntegerField(
            label='Priority',
            initial=default_priority or 0
        )
