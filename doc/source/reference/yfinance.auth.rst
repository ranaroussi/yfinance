=====================
Authentication
=====================

.. currentmodule:: yfinance

.. note::
   The `Auth` module **cannot automate login** using a username and password
   because Yahoo Finance requires solving a **reCAPTCHA**, which blocks automation.
   You must manually obtain and set the authentication cookies.

Class
------------
The `Auth` module, allows you to login to Yahoo! Finance.

.. autosummary::
   :toctree: api/

   Auth

Auth Sample Code
------------------
The `Auth` module, allows you to login to Yahoo! Finance.

.. literalinclude:: examples/auth.py
   :language: python

Obtaining Yahoo Finance Cookies
--------------------------------

To authenticate with Yahoo Finance, you need to obtain specific cookies. Follow these steps:

.. rubric:: Steps to Obtain the Cookies

1. Open your browser (e.g., Chrome, Firefox).
2. Log in to `Yahoo Finance <https://finance.yahoo.com>`_.
3. Open the browser's Developer Tools:

   - Press `F12` or `Ctrl + Shift + I` (Windows/Linux)
   - Press `Cmd + Option + I` (Mac)
4. Navigate to the **Application** tab (Chrome) or **Storage** tab (Firefox).
5. In the **Cookies** section, select `https://finance.yahoo.com`.
6. Locate the cookies named **T** and **Y**.
7. Copy the values of these cookies and pass them to the authentication function.
