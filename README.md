# task_queue

Task Queue handler for Celery

## Warning
This is the Django 5 version. It's not compatible with the Django 3 versions. Upgrading from the Django 3 version
to the Django 5 requires uninstalling completely the previous version.

## Tested with:
- Django==5.0.3
- celery==5.3.6
- cron-descriptor==1.4.3
- django-celery-beat==2.6.0

## Abortable tasks
In order to make tasks abortable, a results
backend must be used for Celery. It won't work with rpc.
This was tested using Redis as backend, and RabbitMQ as broker.

https://docs.celeryq.dev/en/stable/reference/celery.contrib.abortable.html#:%7E:text=Note,the%20database%20backends.