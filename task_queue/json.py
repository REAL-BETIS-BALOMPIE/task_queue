import json

from django.utils.dateparse import parse_datetime, parse_date


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

