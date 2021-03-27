### Test Case Document

**Test Case**
Testing incomplete, null, and broken data for the .earnings and .quarterly_earnings properties of ticker.py for the yfinance module.
Test .earnings and .quarterly_earnings in ticker.py file lines 148 to 154

**Purpose:**
The class TestDataValues in test_earnings.py verifies quarterly and annual earnings are correctly calculated regardless of missing quarterly or annual earnings data, data being null or other missing fields.

**Test Run Information:**
Tester Name(s): Chelsea Hong, Cameron Ridderikhoff
Date(s) of Test: Friday, March 26, 2021

**Prerequisites for this test:**
None

**Software Versions:**
Fork of finance: https://github.com/sazzy4o/yfinance
Python Version: Python 3.4+

**Required Configuration:**
No special setup needed

**Notes and Results:**

| **Step**     | **Test Step/Input** | **Expected Results** | **Actual Results** | **Requirement Validation** | **Pass/Fail** |
| User Flow 1: Verify that the annual .earnings and the .quarterly_earnings properties correctly display information, even with missing and incorrect input data.|
| ------------ | ------------------- | -------------------- | ------------------ | -------------------------- | ------------- |
| 1 | run the test with python3 test_earnings.py |..... --------------------------------------------------- Ran 5 tests in 9.537s OK|..... --------------------------------------------------- Ran 5 tests in 9.537s OK|
The app should not crash even when there is no yearly data, and should still display all available information.

The app should not crash even when there is no quarterly data, or if a quarter is missing, and should still display all available information.

The app should not crash even when there is no annual earnings field data, and should still display all available information.

The app should not crash even when there is no quarterly earnings field data, and should still display all available information.

| ✅ Pass |

<br></br>
