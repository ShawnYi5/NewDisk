from django.db.models import DecimalField


class TimestampField(DecimalField):

    def __init__(self, verbose_name=None, name=None, **kwargs):
        kwargs['max_digits'] = 20
        kwargs['decimal_places'] = 6
        super(TimestampField, self).__init__(verbose_name, name, **kwargs)
