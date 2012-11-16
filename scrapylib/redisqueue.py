try:
    import cPickle as pickle
except ImportError:
    import pickle

from scrapy.exceptions import NotConfigured
from scrapy import signals


class RedisQueue(object):

    def __init__(self, crawler):
        try:
            from redis import Redis
        except ImportError:
            raise NotConfigured

        settings = crawler.settings

        # get settings
        queue = settings.get('REDIS_QUEUE')
        if queue is None:
            raise NotConfigured

        host = settings.get('REDIS_HOST', 'localhost')
        port = settings.getint('REDIS_PORT', 6379)
        db = settings.getint('REDIS_DB', 0)
        password = settings.get('REDIS_PASSWORD')

        self.redis = Redis(host=host, port=port, db=db, password=password)
        self.queue = queue
        self.project = settings['BOT_NAME']

        crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def spider_closed(self, spider, reason):
        msg = {'project': self.project, 'spider': spider.name, 'reason': reason}
        self.redis.rpush(self.queue, pickle.dumps(msg))
