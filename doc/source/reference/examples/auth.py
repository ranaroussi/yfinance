import yfinance as yf
import os

auth = yf.Auth()

# Set login cookies obtained from your browser. The call stores them, validates
# them with a live login check, and returns whether the account is logged in.
if auth.set_login_cookies(os.getenv("COOKIE_T"), os.getenv("COOKIE_Y")):
    print("Logged in")
else:
    print("Invalid or expired cookies")

# Every subsequent request sent to Yahoo Finance is now under the logged-in user.

# Re-check the live login state at any time (re-queried each call, never cached).
auth.check_login()        # -> True / False

# Yahoo Finance subscription tier of the logged-in account.
auth.subscription_tier()  # -> 'gold' / 'silver' / 'bronze' / 'free' / None

# Access user information.
auth.user                 # -> {'guid': ...} or None
