from django.conf import settings

ADMIN_SUCCESS_URL = getattr(settings, 'TQ_ADMIN_SUCCESS_URL', '/admin')
