#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Yahoo! Finance market data downloader (+fix for Pandas Datareader)
# https://github.com/ranaroussi/fix-yahoo-finance

"""Yahoo! Finance market data downloader (+fix for Pandas Datareader)"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='fix-yahoo-finance',
    version="0.1.35",
    description='Yahoo! Finance market data downloader +fix for Pandas Datareader\'s get_data_yahoo()',
    long_description=long_description,
    url='https://github.com/ranaroussi/fix-yahoo-finance',
    author='Ran Aroussi',
    author_email='ran@aroussi.com',
    license='Apache',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        # 'Development Status :: 3 - Alpha',
        # 'Development Status :: 4 - Beta',
        'Development Status :: 5 - Production/Stable',


        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: Office/Business :: Financial',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    platforms = ['any'],
    keywords='pandas, yahoo finance, pandas datareader',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'examples']),
    install_requires=['pandas>=0.24', 'numpy>=1.15',
                      'requests>=2.20', 'multitasking>=0.0.7'],
    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },
)
