from django.utils.translation import gettext as _


class TaskAbortedException(Exception):

    def __init__(self):
        Exception.__init__(self, _('The task was aborted by the queue'))


class CurrentTaskNoneException(Exception):

    def __init__(self):
        Exception.__init__(self, _('Current Task is None'))


class TaskError(Exception):

    def __init__(self, msg=None):
        if msg is None:
            msg = _('There was an error while executing the task')
            Exception.__init__(self, msg)


class QueueWorkerNotRunning(Exception):

    def __init__(self, msg=None):
        if msg is None:
            msg = _('The Queue Worker is not running')
            Exception.__init__(self, msg)
