import yfinance as yf
import os

auth = yf.Auth()

# Set log in cookies from browser
auth.set_login_cookies(os.getenv("COOKIE_T"), os.getenv("COOKIE_Y"))

# Check if the cookies worked
if auth.check_login():
    print("Logged in")
else:
    print("Invalid cookie")

# Every subsequent request sent to Yahoo Finance will now be under the logged-in user.

# Access user information
auth.user
