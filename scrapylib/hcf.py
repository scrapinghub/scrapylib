"""
HCF Middleware

This SpiderMiddleware uses the HCF backend from hubstorage to retrieve the new
urls to crawl and store back the links extracted.
"""
from collections import defaultdict
from scrapy import signals, log
from scrapy.exceptions import NotConfigured
from scrapy.http import Request
from hubstorage import HubstorageClient

META_KEY_SKIP_HCF = 'skip_hcf'
META_KEY_SLOT_CALLBACK = 'slot_callback'


class HcfMiddleware(object):

    def __init__(self, crawler):

        hs_endpoint = self._get_config(crawler, "HS_ENDPOINT")
        hs_auth = self._get_config(crawler, "HS_AUTH")
        hs_projectid = self._get_config(crawler, "HS_PROJECTID")
        self.hs_frontier = self._get_config(crawler, "HS_FRONTIER")
        self.hs_slot = self._get_config(crawler, "HS_SLOT")

        hsclient = HubstorageClient(auth=hs_auth, endpoint=hs_endpoint)
        project = hsclient.get_project(hs_projectid)
        self.fclient = project.frontier

        self.new_links = defaultdict(list)
        self.batch_ids = []

        crawler.signals.connect(self.idle_spider, signals.spider_idle)
        crawler.signals.connect(self.close_spider, signals.spider_closed)

    def _get_config(self, crawler, key):
        value = crawler.settings.get(key)
        if not value:
            raise NotConfigured('%s not found' % key)
        return value

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_start_requests(self, start_requests, spider):
        has_new_links = False
        for link in self._get_new_links():
            has_new_links = True
            yield link

        # if there are no links in the hcf, use the start_requests
        if not has_new_links:
            for r in start_requests:
                yield r

    def process_spider_output(self, response, result, spider):
        skip_hcf = response.meta.get(META_KEY_SKIP_HCF, False)
        slot_callback = response.meta.get(META_KEY_SLOT_CALLBACK, self._get_slot)
        for item in result:
            if isinstance(item, Request) and not skip_hcf:
                request = item
                if request.method == 'GET':  # XXX: Only POST support for now.
                    slot = slot_callback(request)
                    self.new_links[slot].append(request.url)
                else:
                    yield item
            else:
                yield item

    def idle_spider(self, spider):
        self._save_new_links()
        self._delete_processed_ids()
        for link in self._get_new_links():
            yield link

    def close_spider(self, spider):
        self._save_new_links()
        self._delete_processed_ids()
        # XXX: Start new job

    def _get_new_links(self):
        """ Get a new batch of links from the HCF."""
        for batch in self.fclient.read(self.hs_frontier, self.hs_slot):
            self.batch_ids.append(batch['id'])
            for r in batch['requests']:
                yield Request(r[0])

    def _save_new_links(self):
        """ Save the new extracted links into the HCF."""
        for slot, links in self.new_links.items():
            fps = [{'fp': l} for l in links]
            self.fclient.add(self.hs_frontier, self.hs_slot, fps)
        self.new_links = defaultdict(list)

    def _delete_processed_ids(self):
        """ Delete in the HCF the ids of the processed batches."""
        self.fclient.delete(self.hs_frontier, self.hs_slot, self.batch_ids)
        self.batch_ids = []

    def _get_slot(self, request):
        """ Determine to which slot should be saved the request."""
        return 'slot'
