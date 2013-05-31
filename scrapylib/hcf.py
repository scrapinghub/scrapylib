"""
HCF Middleware

This SpiderMiddleware uses the HCF backend from hubstorage to retrieve the new
urls to crawl and store back the links extracted.

To activate this middleware it needs to be added to the SPIDER_MIDDLEWARES 
list, i.e:

SPIDER_MIDDLEWARES = {
    'scrapylib.hcf.HcfMiddleware': 543,
}

And the next settings need to be defined:

    HS_ENDPOINT - URL to the API endpoint, i.e: http://localhost:8003
    HS_AUTH     - API key
    HS_PROJECTID - Project ID in the panel.
    HS_FRONTIER  -  Frontier name.
    HS_SLOT      - Slot from where the spider will read new URLs.
    
The next optional settings can be defined:

    HS_MAX_LINKS - Number of links to be read from the HCF in a single call. 
                   The default is 100.
    HS_START_JOB_ON_CLOSING - If this setting is set to to true when the spider
                              closes and the spider closing reason is one of the
                              following: 'finished', 'closespider_timeout',
                              'closespider_itemcount' or'closespider_pagecount';
                              it will start another job for the same spider. 
"""
from collections import defaultdict
from scrapy import signals, log
from scrapy.exceptions import NotConfigured, DontCloseSpider
from scrapy.http import Request
from hubstorage import HubstorageClient


class HcfMiddleware(object):

    def __init__(self, crawler):

        self.crawler = crawler
        hs_endpoint = self._get_config(crawler, "HS_ENDPOINT")
        hs_auth = self._get_config(crawler, "HS_AUTH")
        self.hs_projectid = self._get_config(crawler, "HS_PROJECTID")
        self.hs_frontier = self._get_config(crawler, "HS_FRONTIER")
        self.hs_slot = self._get_config(crawler, "HS_SLOT")
        # Max number of links to read from the HCF within a single run.
        self.hs_max_links = crawler.settings.get("HS_MAX_LINKS", 100)
        self.hs_start_job_on_closing = crawler.settings.get("HS_START_JOB_ON_CLOSING", False)

        self.hsclient = HubstorageClient(auth=hs_auth, endpoint=hs_endpoint)
        self.project = self.hsclient.get_project(self.hs_projectid)
        self.fclient = self.project.frontier

        self.new_links = defaultdict(list)
        self.batch_ids = []

        crawler.signals.connect(self.idle_spider, signals.spider_idle)
        crawler.signals.connect(self.close_spider, signals.spider_closed)

    def _get_config(self, crawler, key):
        value = crawler.settings.get(key)
        if not value:
            raise NotConfigured('%s not found' % key)
        return value

    def _msg(self, msg):
        log.msg('(HCF) %s' % msg)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_start_requests(self, start_requests, spider):
        has_new_requests = False
        for req in self._get_new_requests():
            has_new_requests = True
            yield req

        # if there are no links in the hcf, use the start_requests
        if not has_new_requests:
            self._msg('Using start_requests')
            for r in start_requests:
                yield r

    def process_spider_output(self, response, result, spider):
        skip_hcf = response.meta.get('skip_hcf', False)
        slot_callback = getattr(spider, 'slot_callback', self._get_slot)
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
        self.fclient.flush()
        self._delete_processed_ids()
        has_new_requests = False
        for request in self._get_new_requests():
            self.crawler.engine.schedule(request, spider)
            has_new_requests = True
        if has_new_requests:
            raise DontCloseSpider

    def close_spider(self, spider, reason):
        # Only store the results if the spider finished normally, if it
        # didn't finished properly there is not way to know whether all the url batches
        # were processed and it is better not to delete them from the frontier
        # (so they will be picked by anothe process).
        if reason == 'finished':
            self._save_new_links()
            self._delete_processed_ids()

        # If the spider finished normally (or it was terminated by a user defined
        # condition) check whether we want to start another job for the same spider.
        if reason in ('finished', 'closespider_timeout',
                      'closespider_itemcount', 'closespider_pagecount'):
            # If this settting is True, starts a new job right after this spider
            # is finished, the idea is to limit every spider runtime (either via
            # itemcount, pagecount or timeout) and then have the old spider start
            # a new one to take its place in the slot.
            if self.hs_start_job_on_closing:
                self._msg("Starting new job" + spider.name)
                job = self.hsclient.start_job(projectid=self.hs_projectid,
                                              spider=spider.name)
                self._msg("New job started: %s" % job)
        self.fclient.close()
        self.hsclient.close()

    def _get_new_requests(self):
        """ Get a new batch of links from the HCF."""
        num_batches = 0
        num_links = 0
        for batch in self.fclient.read(self.hs_frontier, self.hs_slot):
            num_batches += 1
            for r in batch['requests']:
                num_links += 1
                yield Request(r[0])
            self.batch_ids.append(batch['id'])
            if num_links >= self.hs_max_links:
                break
        self._msg('Read %d new batches from slot(%s)' % (num_batches, self.hs_slot))
        self._msg('Read %d new links from slot(%s)' % (num_links, self.hs_slot))

    def _save_new_links(self):
        """ Save the new extracted links into the HCF."""
        for slot, links in self.new_links.items():
            fps = [{'fp': l} for l in links]
            self.fclient.add(self.hs_frontier, slot, fps)
            self._msg('Stored %d new links in slot(%s)' % (len(links), slot))
        self.new_links = defaultdict(list)

    def _delete_processed_ids(self):
        """ Delete in the HCF the ids of the processed batches."""
        self.fclient.delete(self.hs_frontier, self.hs_slot, self.batch_ids)
        self._msg('Deleted %d processed batches in slot(%s)' % (len(self.batch_ids),
                                                                self.hs_slot))
        self.batch_ids = []

    def _get_slot(self, request):
        """ Determine to which slot should be saved the request."""
        return '0'
