try:
    import cPickle as pickle
except ImportError:
    import pickle

from scrapy.conf import settings
from scrapy.exceptions import NotConfigured
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals


class RedisQueue(object):

    def __init__(self):
        try:
            from redis import Redis
        except ImportError:
            raise NotConfigured

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

        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)

    def spider_closed(self, spider, reason):
        msg = {'project': settings['BOT_NAME'], 'spider': spider.name,
               'reason': reason}
        self.redis.rpush(self.queue, pickle.dumps(msg))
