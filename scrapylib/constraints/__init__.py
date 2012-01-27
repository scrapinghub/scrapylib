"""
Item Constrains
---------------

This module provides several classes that can be used as conditions to check
certain item constraints. Conditions are just callables that receive a dict and
*may* raise an AssertionError if the condition is not met.

Item constraints can be checked automatically (at scraping time) to drop items
that fail to meet the constraints. In order to do that, add the constraints
pipeline to your ITEM_PIPELINES:

    ITEM_PIPELINES = ['scrapylib.constraints.pipeline.ConstraintsPipeline']

And define the constraints attribute in your item:

    class Product(Item):
        name = Field()
        price = Field()
        colors = Field()

        constraints = [
            RequiredFields('name', 'price'),
            IsPrice('price'),
            IsList('colors'),
            MinLen(10, 'name'),
        ]

"""

import re
from functools import partial


class RequiredFields(object):
    """Assert that the specified fields are populated and non-empty"""

    def __init__(self, *fields):
        self.fields = fields

    def __call__(self, item):
        for f in self.fields:
            v = item.get(f)
            assert v, "missing field: %s" % f

class IsType(object):
    """Assert that the specified fields are of the given type"""

    def __init__(self, type, *fields):
        self.type = type
        self.fields = fields

    def __call__(self, item):
        for f in self.fields:
            if f in item:
                v = item.get(f)
                assert isinstance(v, self.type), "field %r is not a %s: %r" % \
                    (f, self.type.__name__, v)

IsString = partial(IsType, basestring)
IsUnicode = partial(IsType, unicode)
IsList = partial(IsType, list)
IsDict = partial(IsType, dict)

class IsNumber(object):
    """Assert that the specified fields are string and contain only numbers"""

    def __init__(self, *fields):
        self.fields = fields

    def __call__(self, item):
        for f in self.fields:
            v = item.get(f)
            if v is None:
                continue
            assert isinstance(v, basestring), "field %r is not a string: %r" % (f, v)
            assert v.strip().isdigit(), "field %r contains non-numeric chars: %r" % (f, v)

class IsPrice(object):
    """Assert that the specified fields are string and look like a price"""

    def __init__(self, *fields):
        self.fields = fields
        self.price_re = re.compile('^[0-9\., ]+$')

    def __call__(self, item):
        for f in self.fields:
            v = item.get(f)
            if v:
                assert isinstance(v, basestring), "field %r is not a string: %r" % (f, v)
                assert self.price_re.search(v), "field %r is not a price: %r" % (f, v)

class MaxLen(object):
    """Assert that the length of specified fields do not exceed the given
    size"""

    def __init__(self, size, *fields):
        self.size = size
        self.fields = fields

    def __call__(self, item):
        for f in self.fields:
            v = item.get(f)
            if v:
                self._proper_len(f, v)

    def _proper_len(self, f, v):
        assert len(v) <= self.size, "field %r length exceeds %d: %r" % (f, self.size, v)

class MinLen(MaxLen):
    """Assert that the length of specified fields are larger (or equal) than
    the given size"""

    def _proper_len(self, f, v):
        assert len(v) >= self.size, "field %r length below %d: %r" % (f, self.size, v)
