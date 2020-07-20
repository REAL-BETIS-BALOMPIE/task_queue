from django.db.models import QuerySet

from task_queue.constants import (
    QUEUE_TASK_ABORTED, QUEUE_TASK_CREATED, QUEUE_TASK_DONE, QUEUE_TASK_ERROR, QUEUE_TASK_RUNNING)


class QueueTaskQuerySet(QuerySet):

    def status_created(self):
        return self.filter(
            process_status=QUEUE_TASK_CREATED
        )

    def status_aborted(self):
        return self.filter(
            process_status=QUEUE_TASK_ABORTED
        )

    def status_done(self):
        return self.filter(
            process_status=QUEUE_TASK_DONE
        )

    def status_error(self):
        return self.filter(
            process_status=QUEUE_TASK_ERROR
        )

    def status_running(self):
        return self.filter(
            process_status=QUEUE_TASK_RUNNING
        )

    def are_all_done(self):
        return not self.exclude(process_status=QUEUE_TASK_DONE).exists()
