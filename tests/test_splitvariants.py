""" Tests to cover splitvariants middleware """
from unittest import TestCase

from scrapy.spider import Spider
from scrapy.item import DictItem, Field
from scrapy.http import HtmlResponse
from scrapy.utils.test import get_crawler

from scrapylib.splitvariants import SplitVariantsMiddleware


class TestItem(DictItem):
    """
    Item used in test spider
    """
    fields = {
        'id': Field(),
        'name': Field(),
        'size': Field(),
        'price': Field(),
        'variants': Field()
    }


class SplitVariantsTest(TestCase):
    """ Split variants middleware test cases """
    def setUp(self):
        self.spider = Spider('myspider',
                             start_urls=["http://example.com"])
        self.response = HtmlResponse(body="<html></html>",
                                     url="http://www.example.com")

    def test_variants_splitted(self):
        """
        Checks if item with variants is split as expected
        """
        settings = {"SPLITVARIANTS_ENABLED": True}
        crawler = get_crawler(settings)
        mware = SplitVariantsMiddleware.from_crawler(crawler)

        # Define item with variants
        item = {"id": 12,
                "name": "Big chair",
                "variants": [{"size": "XL", "price": 200},
                             {"size": "L", "price": 220}]}
        result = [TestItem(item)]

        # Define how split items should look
        expected = [
            {"id": 12, "name": "Big chair", "size": 'XL', 'price': 200},
            {"id": 12, "name": "Big chair", "size": 'L', 'price': 220}]

        # Calling middleware for given result
        result = mware.process_spider_output(self.response, result,
                                             self.spider)
        self.assertEquals(list(result), expected)
