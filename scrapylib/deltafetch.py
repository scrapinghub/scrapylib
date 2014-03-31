import os, time

from scrapy.http import Request
from scrapy.item import BaseItem
from scrapy.utils.request import request_fingerprint
from scrapy.utils.project import data_path
from scrapy.exceptions import NotConfigured
from scrapy import log, signals

class DeltaFetch(object):
    """This is a spider middleware to ignore requests to pages containing items
    seen in previous crawls of the same spider, thus producing a "delta crawl"
    containing only new items.

    This also speeds up the crawl, by reducing the number of requests that need
    to be crawled, and processed (typically, item requests are the most cpu
    intensive).

    Supported settings:
    
    * DELTAFETCH_ENABLED - to enable (or disable) this extension
    * DELTAFETCH_DIR - directory where to store state
    * DELTAFETCH_DBM_MODULE - which DBM module to use for storage
    * DELTAFETCH_RESET - reset the state, clearing out all seen requests

    Supported spider arguments:

    * deltafetch_reset - same effect as DELTAFETCH_RESET setting

    Supported request meta keys:

    * deltafetch_key - used to define the lookup key for that request. by
      default it's the fingerprint, but it can be changed to contain an item
      id, for example. This requires support from the spider, but makes the
      extension more efficient for sites that many URLs for the same item.

    """

    def __init__(self, dir, dbmodule='anydbm', reset=False):
        self.dir = dir
        self.dbmodule = __import__(dbmodule)
        self.reset = reset

    @classmethod
    def from_crawler(cls, crawler):
        s = crawler.settings
        if not s.getbool('DELTAFETCH_ENABLED'):
            raise NotConfigured
        dir = data_path(s.get('DELTAFETCH_DIR', 'deltafetch'))
        dbmodule = s.get('DELTAFETCH_DBM_MODULE', 'anydbm')
        reset = s.getbool('DELTAFETCH_RESET')
        o = cls(dir, dbmodule, reset)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider):
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
        dbpath = os.path.join(self.dir, '%s.db' % spider.name)
        reset = self.reset or getattr(spider, 'deltafetch_reset', False)
        flag = 'n' if reset else 'c'
        self.db = self.dbmodule.open(dbpath, flag)

    def spider_closed(self, spider):
        self.db.close()

    def process_spider_output(self, response, result, spider):
        for r in result:
            if isinstance(r, Request):
                key = self._get_key(r)
                if self.db.has_key(key):
                    spider.log("Ignoring already visited: %s" % r, level=log.INFO)
                    continue
            elif isinstance(r, BaseItem):
                key = self._get_key(response.request)
                self.db[key] = str(time.time())
            yield r

    def _get_key(self, request):
        return request.meta.get('deltafetch_key') or request_fingerprint(request)
