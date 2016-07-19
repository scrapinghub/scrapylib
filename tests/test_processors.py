#!/usr/bin/env python
import datetime
import locale
import unittest

from scrapylib.processors import to_datetime, to_date, default_input_processor


def locale_exists():
    current_locale = locale.getlocale(locale.LC_TIME)
    try:
        locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')
    except Exception:
        return False
    else:
        locale.setlocale(locale.LC_TIME, current_locale)
        return True


class TestProcessors(unittest.TestCase):

    def test_to_datetime(self):
        self.assertEquals(to_datetime('March 4, 2011 20:00', '%B %d, %Y %H:%S'),
                          datetime.datetime(2011, 3, 4, 20, 0))

        # test no year in parse format
        test_date = to_datetime('March 4, 20:00', '%B %d, %H:%S')
        self.assertEquals(test_date.year, datetime.datetime.utcnow().year)

        # test parse only date
        self.assertEquals(to_datetime('March 4, 2011', '%B %d, %Y'),
                          datetime.datetime(2011, 3, 4))

    @unittest.skipUnless(locale_exists(), "locale does not exist")
    def test_localized_to_datetime(self):
        current_locale = locale.getlocale(locale.LC_TIME)

        self.assertEquals(
            to_datetime('11 janvier 2011', '%d %B %Y', locale='fr_FR.UTF-8'),
            datetime.datetime(2011, 1, 11)
        )

        self.assertEquals(current_locale, locale.getlocale(locale.LC_TIME))

    def test_to_date(self):
        self.assertEquals(to_date('March 4, 2011', '%B %d, %Y'),
                          datetime.date(2011, 3, 4))

        # test no year in parse format
        test_date = to_date('March 4', '%B %d')
        self.assertEquals(test_date.year, datetime.datetime.utcnow().year)

    @unittest.skipUnless(locale_exists(), "locale does not exist")
    def test_localized_to_date(self):
        current_locale = locale.getlocale(locale.LC_TIME)

        self.assertEquals(
            to_date('11 janvier 2011', '%d %B %Y', locale='fr_FR.UTF-8'),
            datetime.date(2011, 1, 11)
        )

        self.assertEquals(current_locale, locale.getlocale(locale.LC_TIME))

    def test_default_input_processor(self):
        self.assertEquals(default_input_processor(
            """<span id="discount_box" data-option-text="Discount &lt;span class=&quot;
            percent&quot;id=&quot;buy_percent&quot;&gt;&lt;/span&gt;&lt;span class=&quot;
            percent&quot;&gt;%&lt;/span&gt;" data-default-text="up to &lt;span class=&quot;
            percent&quot; id=&quot;buy_percent&quot;&gt;54&lt;/span&gt;&lt;span class=&quot;
            percent&quot;&gt;%&lt;/span&gt;" class="discount">up to <span class="percent"
            id="buy_percent">54</span><span class="percent">%</span></span>"""),
            [u'up to 54%'])

        self.assertEquals(default_input_processor(
            """<p>&lt;&lt; ...The Sunnywale, Calif.-based... &gt;&gt;</p>"""),
            [u'<< ...The Sunnywale, Calif.-based... >>'])

        self.assertEquals(default_input_processor(
            """newline<br>must be replaced before tags and only then quotes like &lt;br>"""),
            [u'newline must be replaced before tags and only then quotes like <br>'])

if __name__ == '__main__':
    unittest.main()
