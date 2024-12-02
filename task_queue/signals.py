import django.dispatch

from django.db import models


def post_delete_queuetask(sender, instance, **kwargs):
    if instance.last_state:
        instance.last_state.delete(save=False)

    if instance.log:
        instance.log.delete(save=False)


def post_delete_queuetaskatnexttasks(sender, instance, **kwargs):
    instance.queue.queuetaskatnexttasks_set.filter(
        position__gt=instance.position
    ).update(
        position=models.F('position') - 1
    )


def post_delete_scheduledqueuetaskgroup(sender, instance, **kwargs):
    if instance.periodic_task:
        instance.periodic_task.delete()


task_done = django.dispatch.Signal(providing_args=['instance'])
