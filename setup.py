from setuptools import setup

setup(
    name='scrapylib',
    version='1.7.1',
    license='BSD',
    description='Scrapy helper functions and processors',
    author='Scrapinghub',
    author_email='info@scrapinghub.com',
    url='https://pypi.python.org/pypi/scrapylib',
    packages=['scrapylib', 'scrapylib.constraints', 'scrapylib.processors'],
    platforms=['Any'],
    classifiers=[
        'Development Status :: 7 - Inactive',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 2 :: Only',
    ],
    install_requires=['Scrapy>=0.22.0']
)
