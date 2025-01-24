#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# yfinance - market data downloader
# https://github.com/ranaroussi/yfinance

"""yfinance - market data downloader"""

from setuptools import setup, find_packages
from setuptools.command.install import install
# from codecs import open
import io
from os import path

# --- get version ---
version = "unknown"
with open("yfinance/version.py") as f:
    line = f.read().strip()
    version = line.replace("version = ", "").replace('"', '')
# --- /get version ---


here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with io.open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

WARNINGS:'dict[str,str]' = {}

class Install(install):
    def run(self):
        if message := WARNINGS.get(version, None):
            print(message)
        install.run(self)





setup(
    name='yfinance',
    version=version,
    description='Download market data from Yahoo! Finance API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/ranaroussi/yfinance',
    author='Ran Aroussi',
    author_email='ran@aroussi.com',
    license='Apache',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        # 'Development Status :: 3 - Alpha',
        'Development Status :: 4 - Beta',
        # 'Development Status :: 5 - Production/Stable',


        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: Office/Business :: Financial',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',

    ],
    platforms=['any'],
    keywords='pandas, yahoo finance, pandas datareader',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'examples']),
    install_requires=['pandas[html]>=1.3.0', 'numpy>=1.16.5',
                      'requests>=2.31', 'multitasking>=0.0.7',
                      'platformdirs>=2.0.0', 'pytz>=2022.5',
                      'frozendict>=2.3.4', 'peewee>=3.16.2',
                      ],
    extras_require={
        'nospam': ['requests_cache>=1.0', 'requests_ratelimiter>=0.3.1'],
        'repair': ['scipy>=1.6.3'],
    },
    # Note: Pandas.read_html() needs html5lib & beautifulsoup4
    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },
)
