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

    HS_AUTH     - API key
    HS_PROJECTID - Project ID in the panel.
    HS_FRONTIER  - Frontier name.
    HS_CONSUME_FROM_SLOT - Slot from where the spider will read new URLs.

Note that HS_FRONTIER and HS_SLOT can be overriden from inside a spider using
the spider attributes: "hs_frontier" and "hs_consume_from_slot" respectively.

The next optional settings can be defined:

    HS_ENDPOINT - URL to the API endpoint, i.e: http://localhost:8003.
                  The default value is provided by the python-hubstorage
                  package.

    HS_MAX_LINKS - Number of links to be read from the HCF, the default is 1000.

    HS_START_JOB_ENABLED - Enable whether to start a new job when the spider
                           finishes. The default is False

    HS_START_JOB_ON_REASON - This is a list of closing reasons, if the spider ends
                             with any of these reasons a new job will be started
                             for the same slot. The default is ['finished']

    HS_START_JOB_NEW_PANEL - If True the jobs will be started in the new panel.
                             The default is False.

    HS_NUMBER_OF_SLOTS - This is the number of slots that the middleware will
                         use to store the new links. The default is 8.

The next keys can be defined in a Request meta in order to control the behavior
of the HCF middleware:

    use_hcf - If set to True the request will be stored in the HCF.
    hcf_params - Dictionary of parameters to be stored in the HCF with the request
                 fingerprint

        qdata    data to be stored along with the fingerprint in the request queue
        fdata    data to be stored along with the fingerprint in the fingerprint set
        p    Priority - lower priority numbers are returned first. The default is 0

The value of 'qdata' parameter could be retrieved later using
``response.meta['hcf_params']['qdata']``.

The spider can override the default slot assignation function by setting the
spider slot_callback method to a function with the following signature:

   def slot_callback(request):
       ...
       return slot

"""
import hashlib
from collections import defaultdict
from datetime import datetime
from scrapinghub import Connection
from scrapy import signals, log
from scrapy.exceptions import NotConfigured
from scrapy.http import Request
from hubstorage import HubstorageClient

DEFAULT_MAX_LINKS = 1000
DEFAULT_HS_NUMBER_OF_SLOTS = 8


class HcfMiddleware(object):

    def __init__(self, crawler):

        self.crawler = crawler
        self.hs_endpoint = crawler.settings.get("HS_ENDPOINT")
        self.hs_auth = self._get_config(crawler, "HS_AUTH")
        self.hs_projectid = self._get_config(crawler, "HS_PROJECTID")
        self.hs_frontier = self._get_config(crawler, "HS_FRONTIER")
        self.hs_consume_from_slot = self._get_config(crawler, "HS_CONSUME_FROM_SLOT")
        try:
            self.hs_number_of_slots = int(crawler.settings.get("HS_NUMBER_OF_SLOTS", DEFAULT_HS_NUMBER_OF_SLOTS))
        except ValueError:
            self.hs_number_of_slots = DEFAULT_HS_NUMBER_OF_SLOTS
        try:
            self.hs_max_links = int(crawler.settings.get("HS_MAX_LINKS", DEFAULT_MAX_LINKS))
        except ValueError:
            self.hs_max_links = DEFAULT_MAX_LINKS
        self.hs_start_job_enabled = crawler.settings.get("HS_START_JOB_ENABLED", False)
        self.hs_start_job_on_reason = crawler.settings.get("HS_START_JOB_ON_REASON", ['finished'])
        self.hs_start_job_new_panel = crawler.settings.get("HS_START_JOB_NEW_PANEL", False)

        if not self.hs_start_job_new_panel:
            conn = Connection(self.hs_auth)
            self.oldpanel_project = conn[self.hs_projectid]

        self.hsclient = HubstorageClient(auth=self.hs_auth, endpoint=self.hs_endpoint)
        self.project = self.hsclient.get_project(self.hs_projectid)
        self.fclient = self.project.frontier

        self.new_links_count = defaultdict(int)
        self.batch_ids = []

        crawler.signals.connect(self.close_spider, signals.spider_closed)

    def _get_config(self, crawler, key):
        value = crawler.settings.get(key)
        if not value:
            raise NotConfigured('%s not found' % key)
        return value

    def _msg(self, msg, level=log.INFO):
        log.msg('(HCF) %s' % msg, level)

    def _start_job(self, spider):
        self._msg("Starting new job for: %s" % spider.name)
        if self.hs_start_job_new_panel:
            jobid = self.hsclient.start_job(projectid=self.hs_projectid,
                                          spider=spider.name)
        else:
            jobid = self.oldpanel_project.schedule(spider.name, slot=self.hs_consume_from_slot,
                                                   dummy=datetime.now())
        self._msg("New job started: %s" % jobid)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_start_requests(self, start_requests, spider):

        self.hs_frontier = getattr(spider, 'hs_frontier', self.hs_frontier)
        self._msg('Using HS_FRONTIER=%s' % self.hs_frontier)

        self.hs_consume_from_slot = getattr(spider, 'hs_consume_from_slot', self.hs_consume_from_slot)
        self._msg('Using HS_CONSUME_FROM_SLOT=%s' % self.hs_consume_from_slot)

        self.has_new_requests = False
        for req in self._get_new_requests():
            self.has_new_requests = True
            yield req

        # if there are no links in the hcf, use the start_requests
        # unless this is not the first job.
        if not self.has_new_requests and not getattr(spider, 'dummy', None):
            self._msg('Using start_requests')
            for r in start_requests:
                yield r

    def process_spider_output(self, response, result, spider):
        slot_callback = getattr(spider, 'slot_callback', self._get_slot)
        for item in result:
            if isinstance(item, Request):
                request = item
                if request.meta.get('use_hcf', False):
                    if request.method == 'GET':  # XXX: Only GET support for now.
                        slot = slot_callback(request)
                        hcf_params = request.meta.get('hcf_params')
                        fp = {'fp': request.url}
                        if hcf_params:
                            fp.update(hcf_params)
                        # Save the new links as soon as possible using
                        # the batch uploader
                        self.fclient.add(self.hs_frontier, slot, [fp])
                        self.new_links_count[slot] += 1
                    else:
                        self._msg("'use_hcf' meta key is not supported for non GET requests (%s)" % request.url,
                                  log.ERROR)
                        yield request
                else:
                    yield request
            else:
                yield item

    def close_spider(self, spider, reason):
        # Only store the results if the spider finished normally, if it
        # didn't finished properly there is not way to know whether all the url batches
        # were processed and it is better not to delete them from the frontier
        # (so they will be picked by another process).
        if reason == 'finished':
            self._save_new_links_count()
            self._delete_processed_ids()

        # Close the frontier client in order to make sure that all the new links
        # are stored.
        self.fclient.close()
        self.hsclient.close()

        # If the reason is defined in the hs_start_job_on_reason list then start
        # a new job right after this spider is finished.
        if self.hs_start_job_enabled and reason in self.hs_start_job_on_reason:

            # Start the new job if this job had requests from the HCF or it
            # was the first job.
            if self.has_new_requests or not getattr(spider, 'dummy', None):
                self._start_job(spider)

    def _get_new_requests(self):
        """ Get a new batch of links from the HCF."""
        num_batches = 0
        num_links = 0
        for num_batches, batch in enumerate(self.fclient.read(self.hs_frontier, self.hs_consume_from_slot), 1):
            for fingerprint, data in batch['requests']:
                num_links += 1
                yield Request(url=fingerprint, meta={'hcf_params': {'qdata': data}})
            self.batch_ids.append(batch['id'])
            if num_links >= self.hs_max_links:
                break
        self._msg('Read %d new batches from slot(%s)' % (num_batches, self.hs_consume_from_slot))
        self._msg('Read %d new links from slot(%s)' % (num_links, self.hs_consume_from_slot))

    def _save_new_links_count(self):
        """ Save the new extracted links into the HCF."""
        for slot, link_count in self.new_links_count.items():
            self._msg('Stored %d new links in slot(%s)' % (link_count, slot))
        self.new_links_count = defaultdict(list)

    def _delete_processed_ids(self):
        """ Delete in the HCF the ids of the processed batches."""
        self.fclient.delete(self.hs_frontier, self.hs_consume_from_slot, self.batch_ids)
        self._msg('Deleted %d processed batches in slot(%s)' % (len(self.batch_ids),
                                                                self.hs_consume_from_slot))
        self.batch_ids = []

    def _get_slot(self, request):
        """ Determine to which slot should be saved the request."""
        md5 = hashlib.md5()
        md5.update(request.url)
        digest = md5.hexdigest()
        return str(int(digest, 16) % self.hs_number_of_slots)
