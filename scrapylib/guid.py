import hashlib

from scrapy import signals
from scrapy.exceptions import DropItem


def hash_values(*values):
    """Hash a series of non-None values.

    For example:
    >>> hash_values('some', 'values', 'to', 'hash')
    '1d7b7a17aeb0e5f9a6814289d12d3253'
    """
    hash = hashlib.md5()
    for value in values:
        if value is None:
            message = "hash_values was passed None at argument index %d" % list(values).index(None)
            raise ValueError(message)
        hash.update('%s' % value)
    return hash.hexdigest()


class GUIDPipeline(object):

    item_fields = {}

    def __init__(self):
        self.guids = {}

    @classmethod
    def from_crawler(cls, crawler):
        o = cls()
        crawler.signals.connect(o.spider_opened, signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signals.spider_closed)
        return o

    def spider_opened(self, spider):
        self.guids[spider] = set()

    def spider_closed(self, spider):
        del self.guids[spider]

    def process_item(self, item, spider):
        if type(item) in self.item_fields:
            item['guid'] = guid = self.generate_guid(item, spider)
            if guid is None:
                raise DropItem("Missing guid fields on: %s" % item)
            if guid in self.guids[spider]:
                raise DropItem("Duplicate item found: %s" % item)
            else:
                self.guids[spider].add(guid)
        return item

    def generate_guid(self, item, spider):
        values = []
        for field in  self.item_fields[type(item)]:
            value = item.get(field)
            if value is None:
                return
            values.append(value.encode('utf-8'))
        values.insert(0, spider.name)
        return hash_values(*values)
