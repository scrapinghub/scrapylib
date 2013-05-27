"""
Allow to add extra fields to items, based on the configuration setting MAGIC_FIELDS.
MAGIC_FIELDS settings is a dict. The keys are the destination field names, their values, a string which admits magic variables,
identified by a starting '$', which will be substituted by a corresponding value. Some magic also accept arguments, and are specified
after the magic name, using a ':' as separator. In case there is more than one argument, they must come separated by ','.
So, the generic magic format is 

$<magic name>[:arg1,arg2,...]

Current magic variables are:
    - $time
            The UTC timestamp at which the item was scraped, in format '%Y-%m-%d %H:%M:%S'.
    - $unixtime
            The unixtime (number of seconds since the Epoch, i.e. time.time()) at which the item was scraped.
    - $isotime
            The UTC timestamp at which the item was scraped, with format '%Y-%m-%dT%H:%M:%S".
    - $spider
            Must be followed by an argument, which is the name of an attribute of the spider (like an argument passed to it).
    - $env
            The value of an environment variable. It admits as argument the name of the variable.
    - $jobid
            The job id (shortcut for $env:SCRAPY_JOB)
    - $jobtime
            The UTC timestamp at which the job started, in format '%Y-%m-%d %H:%M:%S'.
    - $response
            Access to some response properties.
                $response:url
                    The url from where the item was extracted from.
                $response:status
                    Response http status.
                $response:headers
                    Response http headers.
    - $setting
            Access the given Scrapy setting. It accepts one argument: the name of the setting.
    - $url
            Shortcut for $response:url,

Examples:

MAGIC_FIELDS = {'timestamp': 'item scraped at $time', 'spider': '$spider:name'}

The above configuration will add two fields to each scraped item: 'timestamp', which will be filled with the string 'item scraped at <scraped timestamp>',
and 'spider', which will contain the spider name.
"""

import re, time, datetime, os

from scrapy.exceptions import NotConfigured
from scrapy.item import BaseItem

def _time():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

def _isotime():
    return datetime.datetime.utcnow().isoformat()

_ENTITY_FUNCTION_MAP = {
    '$time': _time,
    '$unixtime': time.time,
    '$isotime': _isotime,
}

_ENTITIES_RE = re.compile("(\$[a-z]+)(:\w+)?")
def _format(fmt, spider, fixed_values):
    out = fmt
    for m in _ENTITIES_RE.finditer(fmt):
        val = None
        entity, args = m.groups()
        args = filter(None, (args or ':')[1:].split(','))
        if entity == "$jobid":
            val = os.environ.get('SCRAPY_JOB', '')
        elif entity == "$spider":
            if not hasattr(spider, args[0]):
                spider.log("Error at '%s': spider does not have argument" % m.group())
            else:
                val = str(getattr(spider, args[0]))
        elif entity in fixed_values:
            val = fixed_values[entity]
            if entity == "$setting" and args:
                val = str(val[args[0]])
        elif entity == "$env" and args:
                val = os.environ.get(args[0], '')
        else:
            function = _ENTITY_FUNCTION_MAP.get(entity)
            if function is not None:
                try:
                    val = str(function(*args))
                except:
                    spider.log("Error at '%s': invalid argument for function" % m.group())
        if val is not None:
            out = out.replace(m.group(), val, 1)
    return out

class MagicFieldsMiddleware(object):
    
    @classmethod
    def from_crawler(cls, crawler):
        mfields = crawler.settings.getdict("MAGIC_FIELDS")
        if not mfields:
            raise NotConfigured
        return cls(mfields, settings)

    def __init__(self, mfields, settings):
        self.mfields = mfields
        self.fixed_values = {
            "$jobtime": _time(),
            "$setting": settings,
        }

    def process_spider_output(self, response, result, spider):
        for _res in result:
            if isinstance(_res, BaseItem):
                for field, fmt in self.mfields.items():
                    _res.setdefault(field, _format(fmt, spider, response, self.fixed_values))
            yield _res 

