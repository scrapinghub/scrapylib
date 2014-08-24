from setuptools import setup

setup(
    name='scrapylib',
    version='1.4.0',
    license='BSD',
    description='Scrapy helper functions and processors',
    author='Scrapinghub',
    author_email='info@scrapinghub.com',
    url='http://github.com/scrapinghub/scrapylib',
    packages=['scrapylib', 'scrapylib.constraints', 'scrapylib.processors'],
    platforms=['Any'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python'
    ],
    install_requires=['Scrapy>=0.22.0']
)
