from unittest import TestCase, skipIf

import os
import mock
import tempfile
from scrapy import Request
from scrapy.item import BaseItem
from scrapy.spider import Spider
from scrapy.settings import Settings
from scrapy.exceptions import NotConfigured
from scrapy.utils.request import request_fingerprint
from scrapylib.deltafetch import DeltaFetch


dbmodule = None
try:
    dbmodule = __import__('bsddb3')
except ImportError:
    try:
        dbmodule = __import__('bsddb')
    except ImportError:
        pass


@skipIf(not dbmodule, "bsddb3/bsddb is not found on the system")
class DeltaFetchTestCase(TestCase):

    mwcls = DeltaFetch

    def setUp(self):
        self.spider = Spider('df_tests')
        self.temp_dir = tempfile.gettempdir()
        self.db_path = os.path.join(self.temp_dir, 'df_tests.db')

    def test_init(self):
        # path format is any,  the folder is not created
        instance = self.mwcls('/any/dir', True)
        assert isinstance(instance, self.mwcls)
        self.assertEqual(instance.dir, '/any/dir')
        self.assertEqual(instance.reset, True)

    def test_init_from_crawler(self):
        crawler = mock.Mock()
        # void settings
        crawler.settings = Settings({})
        self.assertRaises(NotConfigured, self.mwcls.from_crawler, crawler)
        with mock.patch('scrapy.utils.project.project_data_dir') as data_dir:
            data_dir.return_value = self.temp_dir

            # simple project_data_dir mock with based settings
            crawler.settings = Settings({'DELTAFETCH_ENABLED': True})
            instance = self.mwcls.from_crawler(crawler)
            assert isinstance(instance, self.mwcls)
            self.assertEqual(
                instance.dir, os.path.join(self.temp_dir, 'deltafetch'))
            self.assertEqual(instance.reset, False)

            # project_data_dir mock with advanced settings
            crawler.settings = Settings({'DELTAFETCH_ENABLED': True,
                                         'DELTAFETCH_DIR': 'other',
                                         'DELTAFETCH_RESET': True})
            instance = self.mwcls.from_crawler(crawler)
            assert isinstance(instance, self.mwcls)
            self.assertEqual(
                instance.dir, os.path.join(self.temp_dir, 'other'))
            self.assertEqual(instance.reset, True)

    def test_spider_opened_new(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        mw = self.mwcls(self.temp_dir, reset=False)
        assert not hasattr(self.mwcls, 'db')
        mw.spider_opened(self.spider)
        assert os.path.isdir(self.temp_dir)
        assert os.path.exists(self.db_path)
        assert hasattr(mw, 'db')
        assert isinstance(mw.db, type(dbmodule.db.DB()))
        assert mw.db.items() == []
        assert mw.db.get_type() == dbmodule.db.DB_HASH
        assert mw.db.get_open_flags() == dbmodule.db.DB_CREATE

    def test_spider_opened_existing(self):
        self._create_test_db()
        mw = self.mwcls(self.temp_dir, reset=False)
        assert not hasattr(self.mwcls, 'db')
        mw.spider_opened(self.spider)
        assert hasattr(mw, 'db')
        assert isinstance(mw.db, type(dbmodule.db.DB()))
        assert mw.db.items() == [('test_key_1', 'test_v_1'),
                                 ('test_key_2', 'test_v_2')]
        assert mw.db.get_type() == dbmodule.db.DB_HASH
        assert mw.db.get_open_flags() == dbmodule.db.DB_CREATE

    def test_spider_opened_existing_spider_reset(self):
        self._create_test_db()
        mw = self.mwcls(self.temp_dir, reset=False)
        assert not hasattr(self.mwcls, 'db')
        self.spider.deltafetch_reset = True
        mw.spider_opened(self.spider)
        assert mw.db.get_open_flags() == dbmodule.db.DB_TRUNCATE

    def test_spider_opened_reset_non_existing_db(self):
        mw = self.mwcls(self.temp_dir, reset=True)
        assert not hasattr(self.mwcls, 'db')
        self.spider.deltafetch_reset = True
        mw.spider_opened(self.spider)
        assert mw.db.fd()
        # there's different logic for different bdb versions:
        # it can fail when opening a non-existing db with truncate flag,
        # then it should be caught and retried with rm & create flag
        assert (mw.db.get_open_flags() == dbmodule.db.DB_CREATE or
                mw.db.get_open_flags() == dbmodule.db.DB_TRUNCATE)

    def test_spider_opened_recreate(self):
        self._create_test_db()
        mw = self.mwcls(self.temp_dir, reset=True)
        assert not hasattr(self.mwcls, 'db')
        mw.spider_opened(self.spider)
        assert hasattr(mw, 'db')
        assert isinstance(mw.db, type(dbmodule.db.DB()))
        assert mw.db.items() == []
        assert mw.db.get_type() == dbmodule.db.DB_HASH
        assert mw.db.get_open_flags() == dbmodule.db.DB_TRUNCATE

    def test_spider_closed(self):
        self._create_test_db()
        mw = self.mwcls(self.temp_dir, reset=True)
        mw.spider_opened(self.spider)
        assert mw.db.fd()
        mw.spider_closed(self.spider)
        self.assertRaises(dbmodule.db.DBError, mw.db.fd)

    def test_process_spider_output(self):
        self._create_test_db()
        mw = self.mwcls(self.temp_dir, reset=False)
        mw.spider_opened(self.spider)
        response = mock.Mock()
        response.request = Request('http://url',
                                   meta={'deltafetch_key': 'key'})
        result = []
        self.assertEqual(list(mw.process_spider_output(
            response, result, self.spider)), [])

        result = [
            Request('http://url', meta={'deltafetch_key': 'key1'}),
            Request('http://url1', meta={'deltafetch_key': 'test_key_1'})
        ]
        self.assertEqual(list(mw.process_spider_output(
            response, result, self.spider)), [result[0]])

        result = [BaseItem(), "not a base item"]
        self.assertEqual(list(mw.process_spider_output(
            response, result, self.spider)), result)
        self.assertEqual(mw.db.keys(), ['test_key_1', 'key', 'test_key_2'])
        assert mw.db['key']

    def test_get_key(self):
        mw = self.mwcls(self.temp_dir, reset=True)
        test_req1 = Request('http://url1')
        self.assertEqual(mw._get_key(test_req1),
                         request_fingerprint(test_req1))
        test_req2 = Request('http://url2', meta={'deltafetch_key': 'dfkey1'})
        self.assertEqual(mw._get_key(test_req2), 'dfkey1')

    def _create_test_db(self):
        db = dbmodule.db.DB()
        # truncate test db if there were failed tests
        db.open(self.db_path, dbmodule.db.DB_HASH,
                dbmodule.db.DB_CREATE | dbmodule.db.DB_TRUNCATE)
        db['test_key_1'] = 'test_v_1'
        db['test_key_2'] = 'test_v_2'
        db.close()
