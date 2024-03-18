import copy
import traceback

from dateutil.relativedelta import relativedelta as rd

from celery import current_app as celery_app
from celery.contrib.abortable import AbortableTask, AbortableAsyncResult
from celery import shared_task as celery_task
from celery.app.task import Task
from celery.utils.log import get_task_logger

from django import forms
from django.utils import timezone

from task_queue.exceptions import TaskAbortedException, TaskError, QueueWorkerNotRunning
from task_queue.helpers import get_default_queue, get_parameters, class_path, get_args
from task_queue.log import LOG_TASK_START_CODE, LOG_ABORTED_CODE, LOG_EXCEPTION_CODE
from task_queue.models import QueueTask, TaskQueue, ScheduledQueueTaskGroup

logger = get_task_logger(__name__)


def _set_up_task(task_class, task_name, *args, existing_id=None, defaults=None, **task_kwargs):
    defaults = defaults or {}
    if existing_id is not None:
        task = QueueTask.objects.update_or_create(
            id=existing_id,
            defaults={
                **defaults,
                'task_class': task_class,
                'task_args': args,
                'task_kwargs': task_kwargs
            }
        )[0]
    else:
        task = QueueTask.objects.create(
            **{
                **defaults,
                'task_class': task_class,
                'task_args': args,
                'task_kwargs': task_kwargs
            }
        )

    return task


def _get_queue(queue_id=None):
    if not queue_id:
        return get_default_queue()
    else:
        return TaskQueue.objects.get(id=queue_id)


class AbstractBaseQueueTask:
    default_priority = 0
    logger = logger
    state = None

    def __init__(self, priority=None):
        self.queue_task_object = None
        self.priority = priority if priority is not None else self.default_priority
        self.state = {}

    def log(self, msg, mode='INFO'):
        if mode == 'INFO':
            self.logger.info(msg)
        elif mode == 'WARNING':
            self.logger.warning(msg)
        elif mode == 'ERROR':
            self.logger.error(msg)

        if self.queue_task_object:
            self.queue_task_object.add_log(f'{mode}: {msg}')

    def raise_error(self, msg):
        self.log(msg, mode='ERROR')
        raise TaskError(msg)

    def from_state(self, prop, default=None):
        return copy.deepcopy(self.state.get(prop, default))

    def save_state(self, **state):
        if self.queue_task_object:
            self.state = copy.deepcopy({
                **self.state,
                **state
            })
            self.queue_task_object.save_state(self.state)

        if hasattr(self, 'is_aborted'):
            if self.is_aborted():
                raise TaskAbortedException()

    def on_error(self):
        pass


class BaseQueueTask(AbstractBaseQueueTask, AbortableTask):
    queue_task_object = None
    task_description = ''
    task_name = "Queued Task"
    task_str = None

    @classmethod
    def get_all_form_fields(cls):
        return {
            **cls.get_form_fields(),
            **cls.get_extra_form_fields()
        }

    @classmethod
    def get_form_fields(cls):
        return {}

    @classmethod
    def get_extra_form_fields(cls):
        """
        This method will typically be used for extra funcionality provided by parent classes
        :return: dict
        """
        return {}

    @classmethod
    def clean_form(cls, cleaned_data):
        return cleaned_data

    @classmethod
    def task_to_str(cls, *args, **kwargs):
        if not cls.task_str:
            return cls.task_name
        # We need to turn args into kwargs
        params = get_parameters(cls.handle)
        filtered_params = list(
            filter(
                lambda x: (
                    x[1].default is x[1].empty and x[1].kind is not x[1].VAR_POSITIONAL
                    and x[1].kind is not x[1].VAR_KEYWORD
                ),
                params
            )
        )
        for index, param_item in enumerate(filtered_params):
            try:
                kwargs[param_item[0]] = args[index]
            except IndexError:
                pass
        form_fields = cls.get_form_fields()
        if form_fields:
            for name, field in form_fields.items():
                if isinstance(field, forms.ChoiceField):
                    if kwargs.get(name):
                        try:
                            kwargs[name] = next(x[1] for x in field.choices if x[0] == kwargs[name])
                        except StopIteration:
                            pass

        return cls.task_str.format(**kwargs)

    @classmethod
    def insert_at_queue(cls, *args, queue_id=None, priority=None, **kwargs):
        priority = priority if priority is not None else cls.default_priority
        task = _set_up_task(
            f'{class_path(cls)}', cls.task_name, *args, defaults={'priority': priority}, **kwargs)
        return _get_queue(queue_id=queue_id).insert_task_by_priority(task)


    @classmethod
    def append_to_queue(cls, *args, queue_id=None, **kwargs):
        task = _set_up_task(
            f'{class_path(cls)}', cls.task_name, *args, **kwargs)
        return _get_queue(queue_id=queue_id).append_task(task)


    @classmethod
    def run_next(cls, *args, queue_id=None, **kwargs):
        task = _set_up_task(
            f'{class_path(cls)}', cls.task_name, *args, **kwargs)
        return _get_queue(queue_id=queue_id).insert_task_at_position(task, 0)


    @classmethod
    def run_now(cls, *args, wait_to_state=False, queue_id=None, **kwargs):
        task = _set_up_task(
            f'{class_path(cls)}', cls.task_name, *args, **kwargs)
        return _get_queue(queue_id=queue_id).run_task(task, wait_to_state=wait_to_state)

    def set_up_db_task(self, *args, existing_id=None, **kwargs):
        defaults = {
            'async_result_id': self.request.id,
        }

        if not existing_id:
            defaults['priority'] = self.priority

        self.queue_task_object = _set_up_task(
            self.name, self.task_name, existing_id=existing_id, defaults=defaults, *args, **kwargs)
        return self.queue_task_object

    def __call__(self, *args, **kwargs):
        """In celery task this function call the run method, here you can
        set some environment variable before the run of the task"""
        task = self.set_up_db_task(*args, existing_id=kwargs.pop('queued_task_id', None), **kwargs)
        self.state = task.get_state()
        super().__call__(*args, **kwargs)

    def run(self, *args, **kwargs):
        """ Processing and importing task main entry point """
        self.log(f'{LOG_TASK_START_CODE}. Queue Task: {self.queue_task_object}')
        try:
            self.queue_task_object.set_status_running()
            self.handle(*args, **kwargs)
            self.queue_task_object.set_status_done()
        except TaskAbortedException as e:
            self.log(f'{LOG_ABORTED_CODE}. Queue Task: {self.queue_task_object} {e}', mode='WARNING')
            self.queue_task_object.set_status_aborted()
            raise e
        except Exception as e:
            self.on_error()
            self.log(f'{LOG_EXCEPTION_CODE}. Queue Task: {self.queue_task_object} {e}', mode='ERROR')
            self.queue_task_object.set_status_error(traceback.format_exc())
            raise e

    def handle(self, *args, **kwargs):
        raise NotImplementedError('handle must be implemented')


class RunScheduledQueueTaskGroup(Task):

    @classmethod
    def full_classname(cls):
        return class_path(cls)

    def run(self, *args, **kwargs):
        scheduled = ScheduledQueueTaskGroup.objects.get(
            id=kwargs['scheduled_id']
        )
        scheduled.run()


celery_app.register_task(RunScheduledQueueTaskGroup())


class BaseQueueMultipleTask(BaseQueueTask):
    tasks = []
    task_run_prefix = '_run_'

    @classmethod
    def get_extra_form_fields(cls):
        fields = {}
        for task in cls.get_tasks():
            fields[f'{cls.task_run_prefix}{task.__name__}'] = forms.BooleanField(
                label=f'Run {task.task_name}',
                required=False,
                initial=True
            )
        return fields

    @classmethod
    def get_tasks(cls):
        return cls.tasks

    def handle(self, *args, **kwargs):
        queue = self.queue_task_object.current_queue
        tasks_to_run = []

        # We do two for loops to remove all the task run booleans from the kwargs
        if not kwargs.get('run_all'):
            for task in reversed(self.get_tasks()):
                if kwargs.pop(f'{self.task_run_prefix}{task.__name__}', None) is True:
                    tasks_to_run.append(task)
        else:
            tasks_to_run = reversed(self.get_tasks())

        for task in tasks_to_run:
            if hasattr(self, f'args_kwargs_{task.__name__}'):
                task_args, task_kwargs = getattr(self, f'args_kwargs_{task.__name__}')(*args, **kwargs)
            else:
                task_args, task_kwargs = args, kwargs
            task_created = task.run_next(*task_args, queue_id=queue.id, **task_kwargs)
            db_task = task_created.task
            db_task.parent_task = self.queue_task_object
            db_task.save()
            self.log(f'Created task {db_task} ({str(db_task.id)})')


issue_detected = None

@celery_task
def check_queues():
    """
    This task will check for every task queue that it's running and the state is correct.
    :return:
    """
    from task_queue.models import TaskQueue, QueueTask

    global issue_detected
    had_issue_detected = issue_detected is not None

    active = celery_app.control.inspect().active()

    for obj in TaskQueue.objects.all():
        try:
            active_tasks = obj.get_active_tasks(celery_active=active)
            if obj.current_task is None and len(active_tasks):
                logger.warn(f'{obj}: Task running and current task None. Killing task.')
                AbortableAsyncResult(active_tasks[0]['id']).revoke(terminate=True)
            elif obj.is_not_working(active_tasks):
                if obj.current_task:
                    if issue_detected == 'not-working':
                        logger.warn(f'{obj}: Current task should be running. Relaunching.')

                        ct = obj.current_task
                        ct.get_celery_task().run_now(
                            *ct.task_args, queue_id=str(obj.id), wait_to_state=False,
                            existing_id=str(ct.id), **ct.task_kwargs
                        )
                    else:
                        issue_detected = 'not-working'
                elif not obj.is_consuming_stopped:
                    if issue_detected == 'not-working':
                        logger.warn(f'{obj}: Next task should be running. Running next task.')
                        obj.run_next()
                    else:
                        issue_detected = 'not-working'

            elif len(active_tasks):
                if obj.current_task and (obj.current_task.async_result_id != active_tasks[0]['id']):
                    if issue_detected == active_tasks[0]['id']:
                        logger.warn(f'Current task is different from task running. Fixing.')
                        real_task_running = QueueTask.objects.filter(async_result_id=active_tasks[0]['id']).first()
                        if not real_task_running:
                            logger.warn(f'{obj}: Weird case. Current Task running does not exist.')
                        else:
                            obj.current_task.set_status_aborted()
                            obj.insert_task_at_position(obj.current_task, 0)
                            obj.current_task = real_task_running
                            obj.save()
                            real_task_running.get_celery_task().run_now(
                                *real_task_running.task_args, queue_id=str(obj.id), wait_to_state=False,
                                existing_id=str(real_task_running.id), **real_task_running.task_kwargs
                            )
                    else:
                        issue_detected = active_tasks[0]['id']
                elif obj.current_task and obj.current_task.last_activity_at < (timezone.now() - rd(hour=1)):
                    logger.warn(f'{obj}: Current Task last activity was more than one hour ago. Restarting...')
                    ct = obj.current_task
                    ct.get_celery_task().run_now(
                        *ct.task_args, queue_id=str(obj.id), wait_to_state=False,
                        existing_id=str(ct.id), **ct.task_kwargs
                    )
            else:
                issue_detected = None

            if had_issue_detected == issue_detected:
                # Issue fixed
                issue_detected = None
            elif had_issue_detected and not issue_detected:
                logger.warn(f'The issue has disappeared')
            elif not had_issue_detected and issue_detected:
                logger.warn(f'Issue was detected. Waiting next execution to fix it')

        except QueueWorkerNotRunning:
            logger.error(f'{obj}: Worker is not running')

