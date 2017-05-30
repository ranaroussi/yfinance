Change Log
===========

0.0.7
-------
- ``pandas_datareader`` is optional (can be called via ``download()`` or via ``pdr.get_data_yahoo()``)
- Tries to re-fetch Yahoo cookie in case of timeout/error

0.0.6
-------
- Forcing index to be of datetime type

0.0.5
-------
- Works using ``requests`` = no need for Selenium, PyVirtualDisplay, or Chrome Driver

0.0.4
-------
- Removed ALL debugging code :)

0.0.3
-------
- Removed debugging code

0.0.2
-------
- Option to explicitly specify the location of the Chrome driver

0.0.1
-------
- Initial release (alpha)