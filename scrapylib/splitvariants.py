"""
Splits each product with variants into different single products.
For autoscraping products adaptation
"""

from copy import deepcopy
from scrapy.item import DictItem
from scrapy.exceptions import NotConfigured

class SplitVariantsMiddleware(object):

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool("SPLITVARIANTS_ENABLED"):
            raise NotConfigured
        return cls()

    def process_spider_output(self, response, result, spider):
        for r in result:
            if isinstance(r, DictItem) and "variants" in r:
                variants = r.pop("variants")
                for variant in variants:
                    new_product = deepcopy(r)
                    new_product.update(variant)
                    yield new_product
            else:
                yield r
