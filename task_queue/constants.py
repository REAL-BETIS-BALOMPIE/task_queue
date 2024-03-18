from django.utils.translation import gettext_lazy as _

DEFAULT_QUEUE_NAME = 'celery'

QUEUE_TASK_CREATED = 'created'
QUEUE_TASK_ABORTED = 'aborted'
QUEUE_TASK_DONE = 'done'
QUEUE_TASK_ERROR = 'error'
QUEUE_TASK_RUNNING = 'running'


QUEUE_TASK_CHOICES = (
    (QUEUE_TASK_CREATED, _('Created')),
    (QUEUE_TASK_ABORTED, _('Aborted')),
    (QUEUE_TASK_DONE, _('Done')),
    (QUEUE_TASK_ERROR, _('Error')),
    (QUEUE_TASK_RUNNING, _('Running')),
)

OPERATION_TYPE_PREFIX = '_op_'

OPERATION_TYPE_APPEND: str = f'{OPERATION_TYPE_PREFIX}append'
OPERATION_TYPE_INSERT: str = f'{OPERATION_TYPE_PREFIX}insert'
OPERATION_TYPE_RUN_NOW: str = f'{OPERATION_TYPE_PREFIX}run'
OPERATION_TYPE_RUN_STATE: str = f'{OPERATION_TYPE_PREFIX}state'
OPERATION_TYPE_RUN_NEXT: str = f'{OPERATION_TYPE_PREFIX}next'

OPERATION_TYPE_CHOICES = (
    (OPERATION_TYPE_APPEND, _('Append to queue end')),
    (OPERATION_TYPE_INSERT, _('Insert by priority')),
    (OPERATION_TYPE_RUN_NOW, _('Run now')),
    (OPERATION_TYPE_RUN_STATE, _('Run on next state save')),
    (OPERATION_TYPE_RUN_NEXT, _('Run after current task')),
)

INTERNAL_PREFIX = '_q_'
