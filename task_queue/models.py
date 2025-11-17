import io
import json
import time

import os
import threading
import uuid

from celery.contrib.abortable import AbortableAsyncResult
from django_celery_beat.models import CrontabSchedule, PeriodicTask

from django.conf import settings
from django.core.files import File
from django.db import models, transaction
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from task_queue.constants import (
    QUEUE_TASK_CHOICES, QUEUE_TASK_CREATED, QUEUE_TASK_RUNNING,
    QUEUE_TASK_ERROR, QUEUE_TASK_DONE, QUEUE_TASK_ABORTED, DEFAULT_QUEUE_NAME,
    OPERATION_TYPE_CHOICES, OPERATION_TYPE_RUN_NEXT, OPERATION_TYPE_RUN_STATE, OPERATION_TYPE_RUN_NOW,
    OPERATION_TYPE_APPEND, OPERATION_TYPE_INSERT
)
from task_queue.exceptions import CurrentTaskNoneException, QueueWorkerNotRunning
from task_queue.helpers import get_celery_task
from task_queue.json import DateJSONDecoder, JSONEncoder
from task_queue.log import LOG_DATE_PATTERN
from task_queue.managers import QueueTaskQuerySet


class BaseModel(models.Model):
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    created = models.DateTimeField(
        verbose_name=_('Created date'),
        auto_now_add=True,
    )
    modified = models.DateTimeField(
        verbose_name=_('Modified date'),
        auto_now=True,
    )

    class Meta:
        abstract = True
        ordering = ('-created', )


class QueueTaskBase(BaseModel):
    task_name = models.CharField(
        verbose_name=_('Task Name'),
        max_length=255,
        default='Queued Task'
    )
    task_class = models.CharField(
        verbose_name=_('Task Class'),
        max_length=100
    )
    task_args = models.JSONField(
        decoder=DateJSONDecoder,
        encoder=JSONEncoder,
        verbose_name=_('Task args'),
        default=list,
        blank=True
    )
    task_kwargs = models.JSONField(
        decoder=DateJSONDecoder,
        encoder=JSONEncoder,
        verbose_name=_('Task kwargs'),
        default=dict,
        blank=True
    )

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        if self._state.adding:
            self.task_name = self._task_to_str()
        super().save(*args, **kwargs)

    def get_celery_task(self):
        return get_celery_task(self.task_class)

    def _task_to_str(self):
        task_str = None
        task_func = get_celery_task(self.task_class)
        if task_func and task_func.task_str:
            kwargs = self.task_kwargs
            try:
                task_str = task_func.task_to_str(self, *self.task_args, **kwargs)
            except (AttributeError, ValueError, IndexError):
                # TODO: Improve exception
                task_str = task_func.task_name
        return task_str


def queue_task_last_state_upload_to(instance, filename):
    return os.path.join(
        'queue_task_state', filename
    )


def queue_task_log_upload_to(instance, filename):
    return os.path.join(
        'queue_task_logs', filename
    )


class QueueTask(QueueTaskBase):
    parent_task = models.ForeignKey(
        verbose_name=_('Parent Task'),
        to='QueueTask',
        related_name='child_tasks',
        blank=True,
        null=True,
        on_delete=models.SET_NULL
    )
    process_status = models.CharField(
        max_length=100,
        choices=QUEUE_TASK_CHOICES,
        default=QUEUE_TASK_CREATED
    )
    priority = models.IntegerField(
        verbose_name=_('Priority'),
        help_text=_('Lower is more priority'),
        default=0
    )
    async_result_id = models.CharField(
        verbose_name=_('Async Result Id'),
        max_length=100,
        null=True,
        blank=True
    )
    last_state = models.FileField(
        verbose_name=_('Last State'),
        upload_to=queue_task_last_state_upload_to,
        blank=True,
        null=True
    )
    last_traceback = models.TextField(
        verbose_name=_('Last Traceback'),
        default='',
        null=True,
        blank=True
    )
    log = models.FileField(
        verbose_name=_('Log'),
        upload_to=queue_task_log_upload_to,
        blank=True,
        null=True
    )
    last_activity_at = models.DateTimeField(
        verbose_name=_('Last Activity At'),
        null=True,
        blank=True
    )
    started_running_at = models.DateTimeField(
        verbose_name=_('Started Running At'),
        null=True,
        blank=True
    )
    finished_at = models.DateTimeField(
        verbose_name=_('Finished At'),
        null=True,
        blank=True
    )

    objects = QueueTaskQuerySet.as_manager()

    log_lock = None

    @property
    def current_queue(self):
        return getattr(self, 'queue', None)

    def __init__(self, *args, **kwargs):
        self.log_lock = threading.Lock()
        self.state_lock = threading.Lock()
        super().__init__(*args, **kwargs)


        if (
            'log' in self.__dict__ and
            self.log and
            not self.log.storage.exists(self.log.name)
        ):
            self.log = None

        if (
            'last_state' in self.__dict__ and
            self.last_state and
            not self.last_state.storage.exists(self.last_state.name)
        ):
            self.last_state = None

    def save(self, *args, **kwargs):
        self.last_activity_at = timezone.now()
        update_fields = kwargs.pop('update_fields', None)
        if update_fields and 'last_activity_at' not in update_fields:
            update_fields = [*update_fields, 'last_activity_at']
        super(QueueTask, self).save(*args, **{**kwargs, **({'update_fields': update_fields} if update_fields else {})})

    def _create_log(self):
        filename = f'{str(self.id)}.log'
        bt = io.BytesIO()
        self.log = File(bt, filename)
        self.save(update_fields=('log', ))

    def add_log(self, msg):
        if self.log_lock.acquire(blocking=True, timeout=15):
            try:
                if not self.log:
                    self._create_log()
                msg = f'[{timezone.now().strftime(LOG_DATE_PATTERN)}]: {msg}\n'
                attempt = 0
                while attempt < 5:
                    try:
                        with self.log.open('a') as log_file:
                            log_file.write(msg)
                    except PermissionError:
                        attempt += 1
                        time.sleep(0.1)
                    else:
                        break

            finally:
                self.log_lock.release()

    def get_state(self):
        self.state_lock.acquire(blocking=True, timeout=15)
        if not self.last_state:
            return {}

        self.last_state.seek(0)
        try:
            res = json.loads(self.last_state.read().decode('utf8'), cls=DateJSONDecoder)
        except json.JSONDecodeError:
            self.last_state.close()
            self.last_state = None
            self.save(update_fields=('last_state', ))
            res = {}
        finally:
            self.state_lock.release()
        return res

    def save_state(self, state: dict):
        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
            try:
                self.state_lock.acquire(blocking=True, timeout=15)
                if state is not None:
                    filename = f'{str(self.id)}.json'
                    bt = io.BytesIO()
                    bt.write(json.dumps(state, cls=JSONEncoder).encode('utf8'))

                    if self.last_state:
                        self.last_state.delete(save=False)

                    self.last_state = File(bt, filename)
                    self.save(update_fields=('last_state', ))
                    self.last_state.close()
                    break
            except OSError as e:
                attempt += 1
                if attempt < max_attempts:
                    print('OSError. Waiting two seconds before retrying')
                    time.sleep(2)
                else:
                    raise e
            finally:
                self.state_lock.release()

    def notify_queue_aborted(self):
        if self.current_queue:
            self.current_queue.on_current_task_aborted()

    def notify_queue_error(self):
        if self.current_queue:
            self.current_queue.on_current_task_error()

    def notify_queue_done(self):
        if self.current_queue:
            self.current_queue.on_current_task_done()

    def set_status_aborted(self):
        self.process_status = QUEUE_TASK_ABORTED
        self.save(update_fields=('process_status', ))
        self.notify_queue_aborted()

    def set_status_error(self, traceback=''):
        self.last_traceback = traceback
        self.process_status = QUEUE_TASK_ERROR
        self.save(update_fields=('process_status', 'last_traceback'))
        self.notify_queue_error()

    def set_status_running(self):
        self.process_status = QUEUE_TASK_RUNNING
        self.started_running_at = timezone.now()
        self.save(update_fields=('process_status', 'started_running_at'))

    def set_status_done(self):
        from task_queue.signals import task_done

        self.process_status = QUEUE_TASK_DONE
        self.finished_at = timezone.now()
        self.save(update_fields=('process_status', 'finished_at'))
        self.notify_queue_done()
        transaction.on_commit(lambda: task_done.send(sender=self.__class__, instance=self))

    def __str__(self):
        return f'{self.task_name} ({self.get_process_status_display()})'

    class Meta:
        ordering = ('priority', 'created')
        verbose_name = _('Queue task')
        verbose_name_plural = _('Queue tasks')


class TaskQueue(BaseModel):
    name = models.CharField(
        verbose_name=_('Name'),
        max_length=255
    )
    current_task = models.OneToOneField(
        verbose_name=_('Current Task'),
        to=QueueTask,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="queue"
    )

    next_tasks = models.ManyToManyField(
        verbose_name=_('Next Tasks'),
        to=QueueTask,
        through='QueueTaskAtNextTasks',
        blank=True,
    )

    failed_tasks = models.ManyToManyField(
        verbose_name=_('Failed Tasks'),
        to=QueueTask,
        blank=True,
        related_name="queues_as_failed"
    )

    aborted_tasks = models.ManyToManyField(
        verbose_name=_('Aborted Tasks'),
        to=QueueTask,
        blank=True,
        related_name="queues_as_aborted"
    )

    done_tasks = models.ManyToManyField(
        verbose_name=_('Tasks Done'),
        to=QueueTask,
        blank=True,
        related_name="queues_as_done"
    )

    state_waiting_task = models.OneToOneField(
        verbose_name=_('State Save Waiting Task'),
        to=QueueTask,
        on_delete=models.SET_NULL,
        related_name="waiting_state_save",
        null=True,
        blank=True,
    )

    celery_queue = models.CharField(
        verbose_name=_('Celery Queue Name'),
        help_text=_('Queue name for Celery Worker. celery is default queue'),
        max_length=100,
        default=DEFAULT_QUEUE_NAME
    )
    is_consuming_stopped = models.BooleanField(
        verbose_name=_('Stop consuming tasks'),
        default=False
    )

    @property
    def ordered_next_tasks(self):
        return self.next_tasks.order_by('queuetaskatnexttasks')

    def _set_current_task(self, task):
        if task is not None:
            # We remove the task from all the queues
            QueueTaskAtNextTasks.objects.filter(
                task=task
            ).delete()
        self.current_task = task
        self.save(update_fields=('current_task', ))

    def on_current_task_done(self):
        self.done_tasks.add(self.current_task)
        self._set_current_task(None)
        if not self.is_consuming_stopped:
            self.run_next()

    def on_current_task_error(self):
        self.failed_tasks.add(self.current_task)
        if not self.is_consuming_stopped:
            self.run_next()

    def on_current_task_aborted(self):
        # we dont add it to aborted as it shouldn't be done in all the cases
        if not self.is_consuming_stopped:
            self.run_next()

    def forbid_consuming(self):
        self.is_consuming_stopped = True
        self.save(update_fields=('is_consuming_stopped', ))

    def allow_consuming(self):
        self.is_consuming_stopped = False
        self.save(update_fields=('is_consuming_stopped', ))

    def insert_task_by_priority(self, task):
        with transaction.atomic():
            same_priority = self.queuetaskatnexttasks_set.filter(
                task__priority=task.priority
            ).last()
            if same_priority:
                return self.insert_task_at_position(task, same_priority.position + 1)
            else:
                next_priority = self.queuetaskatnexttasks_set.filter(
                    task__priority__gt=task.priority
                ).first()
                if next_priority:
                    position = max(next_priority.position, 0)
                    return self.insert_task_at_position(task, position)
                else:
                    return self.append_task(task)

    def insert_task_at_position(self, task, position):
        was_stopped = self.is_consuming_stopped
        self.forbid_consuming()
        t_count = self.next_tasks.count()
        position = min(position, t_count)
        with transaction.atomic():
            if not position == t_count:
                self.queuetaskatnexttasks_set.filter(
                    position__gte=position
                ).update(position=models.F('position') + 1)
            task_created = self.queuetaskatnexttasks_set.update_or_create(
                task=task,
                defaults={
                    'position': position
                }
            )[0]
            if not was_stopped:
                self.allow_consuming()
        self.refresh_from_db()
        if not self.current_task and not self.is_consuming_stopped:
            self.run_next()
        return task_created

    def append_task(self, task):
        position = 0
        last_task = self.queuetaskatnexttasks_set.last()
        if last_task:
            position = last_task.position + 1
        return self.insert_task_at_position(task, position)

    def abort_current_task(self, wait_to_state=False, move_to_aborted=True):
        with transaction.atomic():
            current_task = self.current_task
            async_result = AbortableAsyncResult(current_task.async_result_id)
            if wait_to_state:
                async_result.abort()  # The task will know it's aborted and will finish it's execution
            else:
                async_result.revoke(terminate=True)  # Kill the task
                current_task.set_status_aborted()

            if not move_to_aborted:
                self.insert_task_at_position(current_task, 0)
            else:
                self.aborted_tasks.add(current_task)
            if self.is_consuming_stopped:
                self._set_current_task(None)

    def run_current_task(self):
        task = self.current_task

        if task is None:
            raise CurrentTaskNoneException()

        task_func = get_celery_task(task.task_class)
        if not task_func:
            return task.set_status_error(f'Celery Task {task.task_class} not found')
        args = task.task_args
        kwargs = task.task_kwargs
        kwargs['queued_task_id'] = str(task.id)
        result = task_func.apply_async(queue=self.celery_queue, args=args, kwargs=kwargs, countdown=0.5)
        task.async_result_id = result.id
        task.save(update_fields=('async_result_id', ))

    def run_task(self, task, wait_to_state=False):
        was_stopped = self.is_consuming_stopped
        self.forbid_consuming()
        with transaction.atomic():
            current_task = self.current_task
            if not current_task:
                self._set_current_task(task)
                self.run_current_task()
            else:
                self.abort_current_task(wait_to_state=wait_to_state, move_to_aborted=False)
                if self.state_waiting_task:
                    self.insert_task_at_position(self.state_waiting_task, 0)
                    self.state_waiting_task = None
                    self.save(update_fields=('state_waiting_task', ))
                if wait_to_state:
                    if self.state_waiting_task:
                        # We replace the current waiting for the new one
                        self.insert_task_at_position(self.state_waiting_task, 0)
                    self.state_waiting_task = task
                    self.save(update_fields=('state_waiting_task', ))
                else:
                    self._set_current_task(task)
                    self.run_current_task()
        if not was_stopped:
            self.allow_consuming()
        return task

    def run_next(self):
        with transaction.atomic():
            if self.state_waiting_task:
                next_task = self.state_waiting_task
                self.state_waiting_task = None
            else:
                next_task = getattr(self.queuetaskatnexttasks_set.first(), 'task', None)
            self._set_current_task(next_task)
            if next_task:
                self.run_current_task()

    # Control methods
    def get_active_tasks(self, celery_active=None, raise_exception=True):
        """
        Get current tasks at celery
        :param celery_active: Celery active tasks. It's a heavy method, that's why it can be passed as param
        :param raise_exception: Raise QueueWorkerNotRunning exception
        :return: list of tasks, empty list, (None or exception depending on raise_exception)
        """
        from celery import current_app as celery_app

        if not celery_active:
            celery_active = celery_app.control.inspect().active()
        try:
            key = next(key for key in celery_active.keys() if key.startswith(self.celery_queue))
            return celery_active[key]
        except (StopIteration, AttributeError):
            if raise_exception:
                raise QueueWorkerNotRunning()
            return None

    def is_not_working(self, active_tasks=None):
        if not active_tasks:
            active_tasks = self.get_active_tasks(raise_exception=True)

        if len(active_tasks) == 0 and (
                self.current_task and self.current_task.process_status in [QUEUE_TASK_CREATED, QUEUE_TASK_RUNNING]) or (
                    not self.current_task and (self.next_tasks.exists() or self.state_waiting_task)):
            return True
        return False

    def __str__(self):
        return f'Queue: {self.name} ({self.celery_queue})'


class QueueTaskAtNextTasks(models.Model):
    queue = models.ForeignKey(
        verbose_name=_('Queue'),
        to=TaskQueue,
        on_delete=models.CASCADE
    )
    task = models.ForeignKey(
        verbose_name=_('Task'),
        to=QueueTask,
        on_delete=models.CASCADE
    )
    position = models.PositiveIntegerField(
        verbose_name=_('Position')
    )

    class Meta:
        ordering = ('position', 'task')
        verbose_name = _('Task at Queue Next Tasks')
        verbose_name_plural = _('Tasks at Queue Next Tasks')


class QueueTaskTemplate(QueueTaskBase):

    class Meta:
        ordering = ('created', )
        verbose_name = _('Queue Task Template')
        verbose_name_plural = _('Queue Task Template')

    def create_task(self, save=True):
        payload = {
            'task_name': self.task_name,
            'task_class': self.task_class,
            'task_args': self.task_args,
            'task_kwargs': self.task_kwargs
        }
        if save:
            return TaskQueue.objects.create(
                **payload
            )
        else:
            return TaskQueue(**payload)

    def append_to_queue(self, queue_id):
        task_func = self.get_celery_task()
        task_func.append_to_queue(
            *self.task_args,
            queue_id=str(queue_id),
            **self.task_kwargs
        )

    def insert_at_queue(self, queue_id, priority=0):
        task_func = self.get_celery_task()
        task_func.insert_at_queue(
            *self.task_args,
            queue_id=str(queue_id),
            priority=priority,
            **self.task_kwargs
        )

    def run_next(self, queue_id):
        task_func = self.get_celery_task()
        task_func.run_next(
            *self.task_args,
            queue_id=str(queue_id),
            **self.task_kwargs
        )

    def run_now(self, queue_id, wait_to_state=False):
        task_func = self.get_celery_task()
        task_func.run_now(
            *self.task_args,
            wait_to_state=wait_to_state,
            queue_id=str(queue_id),
            **self.task_kwargs
        )

    def __str__(self):
        return f'{self.task_name}  (TEMPLATE)'


class QueueTaskGroup(BaseModel):
    name = models.CharField(
        verbose_name=_('Name'),
        max_length=255
    )
    group_groups = models.ManyToManyField(
        verbose_name=_('Task Groups'),
        to='task_queue.QueueTaskGroup',
        through='QueueTaskGroupAtGroup',
        blank=True
    )
    group_tasks = models.ManyToManyField(
        verbose_name=_('Tasks'),
        to=QueueTaskTemplate,
        through='QueueTaskTemplateAtGroup',
        blank=True
    )
    priority = models.IntegerField(
        verbose_name=_('Tasks Priority'),
        blank=True,
        null=True
    )

    class Meta:
        ordering = ('name', 'created')
        verbose_name = _('Queue Task Group')
        verbose_name_plural = _('Queue Task Groups')

    @property
    def ordered_tasks(self):
        tasks = []
        groups = self.group_groups.all().prefetch_related('group_groups', 'group_tasks')
        for group in groups:
            tasks += list(group.ordered_tasks)
        tasks += list(self.ordered_group_tasks)
        return tasks

    @property
    def ordered_group_tasks(self):
        return self.group_tasks.order_by('queuetasktemplateatgroup')

    def perform_operation(self, queue_id, operation, priority=None):
        if operation == OPERATION_TYPE_INSERT:
            self.insert_at_queue(queue_id, priority=priority)
        if operation == OPERATION_TYPE_APPEND:
            self.append_to_queue(queue_id)
        elif operation == OPERATION_TYPE_RUN_NEXT:
            self.run_next(queue_id)
        elif operation in [OPERATION_TYPE_RUN_STATE, OPERATION_TYPE_RUN_NOW]:
            self.run_now(queue_id, wait_to_state=operation == OPERATION_TYPE_RUN_STATE)

    def append_to_queue(self, queue_id):
        with transaction.atomic():
            for task in self.ordered_tasks:
                task.append_to_queue(queue_id)

    def insert_at_queue(self, queue_id, priority=None):
        priority = priority if priority is not None else self.priority
        with transaction.atomic():
            for task in self.ordered_tasks:
                task.insert_at_queue(queue_id, priority=priority)

    def run_next(self, queue_id):
        with transaction.atomic():
            tasks = reversed(self.ordered_tasks)
            for task in tasks:
                task.run_next(queue_id)

    def run_now(self, queue_id, wait_to_state=False):
        with transaction.atomic():
            tasks = self.ordered_group_tasks
            if tasks:
                tasks[0].run_now(
                    queue_id,
                    wait_to_state=wait_to_state,
                )
            for task in reversed(tasks[1:]):
                task.run_next(queue_id)

    def __str__(self):
        return f'{self.name}. ({len(self.ordered_tasks)} tasks)'


class QueueTaskTemplateAtGroup(models.Model):
    template = models.ForeignKey(
        verbose_name=_('Queue Task Template'),
        to=QueueTaskTemplate,
        on_delete=models.CASCADE
    )
    group = models.ForeignKey(
        verbose_name=_('Queue Task Group'),
        to=QueueTaskGroup,
        on_delete=models.CASCADE
    )
    order = models.PositiveIntegerField(
        verbose_name=_('Order'),
    )

    class Meta:
        ordering = ('order', 'template')
        verbose_name = _('Queue Task At Group')
        verbose_name_plural = _('Queue Tasks At Group')


class QueueTaskGroupAtGroup(models.Model):
    parent_group = models.ForeignKey(
        verbose_name=_('Queue Task Group'),
        to=QueueTaskGroup,
        related_name='groups',
        on_delete=models.CASCADE
    )
    children_group = models.ForeignKey(
        verbose_name=_('Queue Task Group'),
        to=QueueTaskGroup,
        related_name='as_children_group',
        on_delete=models.CASCADE
    )
    order = models.PositiveIntegerField(
        verbose_name=_('Order'),
    )

    class Meta:
        ordering = ('order', 'children_group')
        verbose_name = _('Queue Task At Group')
        verbose_name_plural = _('Queue Tasks At Group')


class ScheduledQueueTaskGroup(BaseModel):
    task_group = models.ForeignKey(
        verbose_name=_('Queue Task Group'),
        to=QueueTaskGroup,
        on_delete=models.CASCADE
    )
    queue = models.ForeignKey(
        verbose_name=_('Queue'),
        to=TaskQueue,
        on_delete=models.SET_NULL,
        null=True,
        blank=False # not an error
    )
    priority = models.IntegerField(
        verbose_name=_('Priority'),
        null=True,
        blank=True
    )
    operation_type = models.CharField(
        verbose_name=_('Operation Type'),
        max_length=25,
        choices=OPERATION_TYPE_CHOICES
    )
    crontab_schedule = models.OneToOneField(
        verbose_name=_('Crontab Schedule'),
        to=CrontabSchedule,
        on_delete=models.CASCADE
    )
    periodic_task = models.OneToOneField(
        verbose_name=_('Periodic Task'),
        to=PeriodicTask,
        on_delete=models.CASCADE,
        null=True,
        blank=True
    )
    disabled = models.BooleanField(
        verbose_name=_('Disabled'),
        help_text=_('If disabled, the task wont be executed'),
        default=False
    )

    class Meta:
        ordering = ('task_group', )
        verbose_name = _('Scheduled Queue Task Group')
        verbose_name_plural = _('Scheduled Queue Task Groups')

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):
        from task_queue.tasks import RunScheduledQueueTaskGroup
        if not self.periodic_task:
            name = '{}_{}'.format(
                self.task_group.name,
                str(uuid.uuid4()),
            )

            pt = PeriodicTask.objects.create(
                crontab=self.crontab_schedule,
                name=name,
                task=RunScheduledQueueTaskGroup.full_classname(),
                kwargs=json.dumps({
                    'scheduled_id': str(self.id)
                }),
                enabled=not self.disabled,
                queue=getattr(settings, 'CELERY_PERIODIC_QUEUE', 'periodic')
            )
            self.periodic_task = pt
        else:
            if self.periodic_task.enabled != (not self.disabled):
                pt = self.periodic_task
                pt.enabled = not self.disabled
                pt.save()

        return super().save(
            force_insert=force_insert, force_update=force_update, using=using, update_fields=update_fields)

    def get_cron_description(self):
        from task_queue.helpers import cron_to_human

        minute = self.crontab_schedule.minute
        hour = self.crontab_schedule.hour
        day_of_month = self.crontab_schedule.day_of_month
        month_of_year = self.crontab_schedule.month_of_year
        day_of_week = self.crontab_schedule.day_of_week
        return cron_to_human(" ".join([minute, hour, day_of_month, month_of_year, day_of_week]))

    def run(self):
        self.task_group.perform_operation(self.queue_id, self.operation_type, self.priority)

    def __str__(self):
        return f'{self.task_group}. {self.get_cron_description()}'
