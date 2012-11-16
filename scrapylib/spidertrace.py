"""
Spider Trace 

This SpiderMiddleware logs a trace of requests and items extracted for a
spider
"""
import os
from os.path import basename
from tempfile import mkstemp
from gzip import GzipFile
import time
import boto
import json
from boto.s3.key import Key
from scrapy import signals, log
from scrapy.exceptions import NotConfigured
from scrapy.http import Request
from scrapy.utils.request import request_fingerprint


class SpiderTraceMiddleware(object):
    """Saves a trace of spider execution and uploads to S3

    The trace records:
        (timestamp, http response, results extracted from spider)
    """
    REQUEST_ATTRS = ('url', 'method', 'body', 'headers', 'cookies', 'meta')
    RESPONSE_ATTRS = ('url', 'status', 'headers', 'body', 'request', 'flags')

    def __init__(self, crawler):
        self.bucket = crawler.settings.get("SPIDERTRACE_BUCKET")
        if not self.bucket:
            raise NotConfigured
        crawler.signals.connect(self.open_spider, signals.spider_opened)
        crawler.signals.connect(self.close_spider, signals.spider_closed)
        self.outputs = {}

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_spider_output(self, response, result, spider):
        f = self.outputs[spider]
        fp = request_fingerprint(response.request)
        tracetime = time.time()
        data = self._objtodict(self.RESPONSE_ATTRS, response)
        data['request'] = self._objtodict(self.REQUEST_ATTRS, response.request)
        self._write(f, fp, tracetime, 'response', data)

        for item in result:
            if isinstance(item, Request):
                data = self._objtodict(self.REQUEST_ATTRS, item)
                data['fp'] = request_fingerprint(item)
                self._write(f, fp, tracetime, 'request', data)
            else:
                self._write(f, fp, tracetime, 'item', dict(item))
            yield item

    @staticmethod
    def _write(f, fp, tracetime, otype, data):
        f.write('%s\t%s\t%s\t%s\n' % (tracetime, fp, otype, json.dumps(data)))

    @staticmethod
    def _objtodict(attrs, obj):
        data = [(a, getattr(obj, a)) for a in attrs]
        return dict(x for x in data if x[1])

    def open_spider(self, spider):
        _, fname = mkstemp(prefix=spider.name + '-', suffix='.trace.gz')
        self.outputs[spider] = GzipFile(fname, 'wb')

    def close_spider(self, spider):
        f = self.outputs.pop(spider)
        f.close()
        c = boto.connect_s3()
        fname = basename(f.name)
        key = Key(c.get_bucket(self.bucket), fname)
        log.msg("uploading trace to s3://%s/%s" % (key.bucket.name, fname))
        key.set_contents_from_filename(f.name)
        os.remove(f.name)
