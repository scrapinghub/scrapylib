"""Get parameter cleaner for AS.

Add removed/kept pattern (regex) with

QUERYCLEANER_REMOVE
QUERYCLEANER_KEEP

Remove patterns has precedence.
"""
import re
from urllib import quote

from scrapy.utils.httpobj import urlparse_cached
from scrapy.http import Request
from scrapy.exceptions import NotConfigured

from w3lib.url import _safe_chars

def _parse_query_string(query):
    """Used for replacing cgi.parse_qsl.
    The cgi version returns the same pair for query 'key'
    and query 'key=', so reconstruction
    maps to the same string. But some sites does not handle both versions
    in the same way.
    This version returns (key, None) in the first case, and (key, '') in the
    second one, so correct reconstruction can be performed."""

    params = query.split("&")
    keyvals = []
    for param in params:
        kv = param.split("=") + [None]
        keyvals.append((kv[0], kv[1]))
    return keyvals

def _filter_query(query, remove_re=None, keep_re=None):
    """
    Filters query parameters in a query string according to key patterns
    >>> _filter_query('as=3&bs=8&cs=9')
    'as=3&bs=8&cs=9'
    >>> _filter_query('as=3&bs=8&cs=9', None, re.compile("as|bs"))
    'as=3&bs=8'
    >>> _filter_query('as=3&bs=8&cs=9', re.compile("as|bs"))
    'cs=9'
    >>> _filter_query('as=3&bs=8&cs=9', re.compile("as|bs"), re.compile("as|cs"))
    'cs=9'
    """
    keyvals = _parse_query_string(query)
    qargs = []
    for k, v in keyvals:
        if remove_re is not None and remove_re.search(k):
            continue
        if keep_re is None or keep_re.search(k):
            qarg = quote(k, _safe_chars)
            if isinstance(v, basestring):
                qarg = qarg + '=' + quote(v, _safe_chars)
            qargs.append(qarg.replace("%20", "+"))
    return '&'.join(qargs)

class QueryCleanerMiddleware(object):
    def __init__(self, settings):
        remove = settings.get("QUERYCLEANER_REMOVE")
        keep = settings.get("QUERYCLEANER_KEEP")
        if not (remove or keep):
            raise NotConfigured
        self.remove = re.compile(remove) if remove else None
        self.keep = re.compile(keep) if keep else None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_spider_output(self, response, result, spider):
        for res in result:
            if isinstance(res, Request):
                parsed = urlparse_cached(res)
                if parsed.query:
                    parsed = parsed._replace(query=_filter_query(parsed.query, self.remove, self.keep))
                    res = res.replace(url=parsed.geturl())
            yield res

