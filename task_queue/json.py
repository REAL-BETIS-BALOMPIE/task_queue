import json
import contextlib
import datetime
import decimal
import json  # noqa
import uuid

from django.db.models.query import QuerySet
from django.utils import timezone
from django.utils.dateparse import parse_datetime, parse_date
from django.utils.encoding import force_str
from django.utils.functional import Promise


###
# This Encoder is copied from Django Rest Framework
# https://github.com/encode/django-rest-framework/blob/337ba211e82628c0a0bfc7340c45b22e725f8162/rest_framework/utils/encoders.py
###
class JSONEncoder(json.JSONEncoder):
    """
    JSONEncoder subclass that knows how to encode date/time/timedelta,
    decimal types, generators and other basic python objects.
    """
    def default(self, obj):
        # For Date Time string spec, see ECMA 262
        # https://ecma-international.org/ecma-262/5.1/#sec-15.9.1.15
        if isinstance(obj, Promise):
            return force_str(obj)
        elif isinstance(obj, datetime.datetime):
            representation = obj.isoformat()
            if representation.endswith('+00:00'):
                representation = representation[:-6] + 'Z'
            return representation
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, datetime.time):
            if timezone and timezone.is_aware(obj):
                raise ValueError("JSON can't represent timezone-aware times.")
            representation = obj.isoformat()
            return representation
        elif isinstance(obj, datetime.timedelta):
            return str(obj.total_seconds())
        elif isinstance(obj, decimal.Decimal):
            # Serializers will coerce decimals to strings by default.
            return float(obj)
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, QuerySet):
            return tuple(obj)
        elif isinstance(obj, bytes):
            # Best-effort for binary blobs. See #4187.
            return obj.decode()
        elif hasattr(obj, 'tolist'):
            # Numpy arrays and array scalars.
            return obj.tolist()
        elif hasattr(obj, '__getitem__'):
            cls = (list if isinstance(obj, (list, tuple)) else dict)
            with contextlib.suppress(Exception):
                return cls(obj)
        elif hasattr(obj, '__iter__'):
            return tuple(item for item in obj)
        return super().default(obj)


class DateJSONDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.test_date, *args, **kwargs)

    @staticmethod
    def _try_decode(val):
        try:
            dt = parse_datetime(val)
            if not dt:
                dt = parse_date(val)
            if dt:
                return dt
            return val
        except ValueError:
            return val

    def test_date(self, obj):
        if isinstance(obj, str):
            return self._try_decode(obj)
        elif isinstance(obj, dict):
            for key in obj.keys():
                obj[key] = self.test_date(obj[key])
        elif isinstance(obj, list):
            obj = list(map(lambda x: self.test_date(x), obj))
        return obj

