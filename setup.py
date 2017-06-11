#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Yahoo! Finance Fix for Pandas Datareader
# https://github.com/ranaroussi/fix-yahoo-finance

"""Yahoo! Finance Fix for Pandas Datareader"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='fix-yahoo-finance',
    version="0.0.12",
    description='Temporary fix for Pandas Datareader\'s get_data_yahoo()',
    long_description=long_description,
    url='https://github.com/ranaroussi/fix-yahoo-finance',
    author='Ran Aroussi',
    author_email='ran@aroussi.com',
    license='LGPL',
    classifiers=[
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Development Status :: 3 - Alpha',

        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: Office/Business :: Financial',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    platforms = ['any'],
    keywords='pandas, yahoo finance, pandas datareader',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'examples']),
    install_requires=['pandas', 'numpy', 'requests', 'multitasking'],
    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },

    # include_package_data=True,
    # package_data={
    #     'static': 'yahoo_finance_fix/Adblock-Plus_v1.11.crx'
    # }
)