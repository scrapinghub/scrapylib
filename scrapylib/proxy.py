import base64
from urllib import unquote
from urllib2 import _parse_proxy
from urlparse import urlunparse


class SelectiveProxyMiddleware(object):
    """A middleware to enable http proxy to selected spiders only.

    Settings:
        HTTP_PROXY -- proxy uri. e.g.: http://user:pass@proxy.host:port
        PROXY_SPIDERS -- all requests from these spiders will be routed
                         through the proxy
    """

    def __init__(self, settings):
        self.proxy = self.parse_proxy(settings.get('HTTP_PROXY'), 'http')
        self.proxy_spiders = set(settings.getlist('PROXY_SPIDERS', []))

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def parse_proxy(self, url, orig_type):
        proxy_type, user, password, hostport = _parse_proxy(url)
        proxy_url = urlunparse((proxy_type or orig_type, hostport, '', '', '', ''))

        if user and password:
            user_pass = '%s:%s' % (unquote(user), unquote(password))
            creds = base64.b64encode(user_pass).strip()
        else:
            creds = None

        return creds, proxy_url

    def process_request(self, request, spider):
        if spider.name in self.proxy_spiders:
            creds, proxy = self.proxy
            request.meta['proxy'] = proxy
            if creds:
                request.headers['Proxy-Authorization'] = 'Basic ' + creds
