from celery import current_app as celery_app

from django.core.checks import Error, register


@register()
def example_check(app_configs, **kwargs):
    from task_queue.helpers import get_parameters
    from task_queue.tasks import BaseQueueTask

    errors = []
    tasks = filter(
        lambda x: isinstance(x, BaseQueueTask),
        celery_app.tasks.values()
    )

    for task in tasks:
        params = get_parameters(task.handle)
        params = [p[0] for p in params]
        if 'kwargs' not in params:
            errors.append(
                Error(
                    'Handle must always include **kwargs',
                    hint='',
                    obj=task,
                    id='task_queue.E001',
                )
            )
    return errors
