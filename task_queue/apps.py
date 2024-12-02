from django.apps import AppConfig
from django.db.models.signals import post_delete


class TaskQueueConfig(AppConfig):
    name = 'task_queue'

    def ready(self):
        import task_queue.checks
        import task_queue.tasks

        from task_queue.models import QueueTask, QueueTaskAtNextTasks
        from task_queue.signals import (
            post_delete_queuetaskatnexttasks,
            post_delete_queuetask
        )

        post_delete.connect(post_delete_queuetask, sender=QueueTask)
        post_delete.connect(post_delete_queuetaskatnexttasks, sender=QueueTaskAtNextTasks)
