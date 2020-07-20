import inspect

from celery import current_app as celery_app
from cron_descriptor import get_description

from django.db.models import Count

from task_queue.constants import DEFAULT_QUEUE_NAME


def get_default_queues_queryset():
    from task_queue.models import TaskQueue

    return TaskQueue.objects.filter(celery_queue=DEFAULT_QUEUE_NAME).annotate(
        next_tasks_count=Count('next_tasks')
    ).order_by('next_tasks_count')


def get_default_queue():
    return get_default_queues_queryset().first()


def cron_to_human(cron):
    return str(get_description(cron))


def get_celery_task(task_class):
    celery = celery_app
    celery.autodiscover_tasks()

    return celery_app.tasks.get(task_class)


def get_parameters(func):
    return list(inspect.signature(func).parameters.items())


def get_args(func):
    return [p for p in get_parameters(func) if p[1].default is inspect._empty and p[0] not in ['self', 'kwargs']]


def class_path(cls):
    return f'{cls.__module__}.{cls.__name__}'
