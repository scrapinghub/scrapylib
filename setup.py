try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='scextras',
      version='0.1',
      license='BSD',
      description='Scrapy helper functions and processors',
      author='Scrapinghub',
      author_email='info@scrapinghub.com',
      url='http://github.com/scrapinghub/scextras',
      packages=['scextras', 'scextras.processors'],
      platforms = ['Any'],
      classifiers = [ 'Development Status :: 4 - Beta',
                      'License :: OSI Approved :: BSD License',
                      'Operating System :: OS Independent',
                      'Programming Language :: Python']
)
