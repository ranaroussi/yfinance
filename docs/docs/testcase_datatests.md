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

<table class="c15"><tbody><tr class="c6"><td class="c3" colspan="1" rowspan="1"><p class="c1"><span class="c0"><b>Step</b></span></p></td><td class="c14" colspan="1" rowspan="1"><p class="c1"><span class="c0"><b>Test Step/Input</b></span></p></td><td class="c8" colspan="1" rowspan="1"><p class="c1"><span class="c0"><b>Expected Results</b></span></p></td><td class="c8" colspan="1" rowspan="1"><p class="c1"><span class="c0"><b>Actual Results</b></span></p></td><td class="c16" colspan="1" rowspan="1"><p class="c1"><span class="c0"><b>Requirements Validation</b></span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c1"><span class="c0"><b>Pass/Fail</b></span></p></td></tr><tr class="c19"><td class="c9" colspan="6" rowspan="1"><p class="c1"><span class="c0">User Flow 1: Verify that the annual .earnings and the .quarterly_earnings properties correctly display information, even with missing and incorrect input data.</span></p></td></tr><tr class="c6"><td class="c3" colspan="1" rowspan="1"><p class="c1"><span class="c0">1.</span></p></td><td class="c14" colspan="1" rowspan="1"><p class="c1"><span>run the test with </span><span class="c11">python3 test_earnings.py</span></p></td><td class="c8" colspan="1" rowspan="1"><p class="c1"><span class="c12 c11">.....</span></p><p class="c1"><span class="c12 c11">---------------------------------------------------</span></p><p class="c1"><span class="c12 c11">Ran 5 tests in 9.537s</span></p><p class="c1 c10"><span class="c12 c11"></span></p><p class="c1"><span class="c11">OK</span></p></td><td class="c8" colspan="1" rowspan="1"><p class="c1"><span class="c11 c12">.....</span></p><p class="c1"><span class="c12 c11">---------------------------------------------------</span></p><p class="c1"><span class="c12 c11">Ran 5 tests in 9.537s</span></p><p class="c1 c10"><span class="c12 c11"></span></p><p class="c1"><span class="c11">OK</span></p></td><td class="c16" colspan="1" rowspan="1"><p class="c1"><span class="c0">The app should not crash even when there is no yearly data, and should still display all available information.</span></p><p class="c1 c10"><span class="c0"></span></p><p class="c1"><span class="c0">The app should not crash even when there is no quarterly data, or if a quarter is missing, and should still display all available information.</span></p><p class="c1 c10"><span class="c0"></span></p><p class="c1"><span class="c0">The app should not crash even when there is no annual earnings field data, and should still display all available information.</span></p><p class="c1 c10"><span class="c0"></span></p><p class="c1"><span class="c0">The app should not crash even when there is no quarterly earnings field data, and should still display all available information.</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c1"><span class="c0">Pass</span></p></td></tr><tr class="c19"><td class="c9" colspan="6" rowspan="1"><p class="c1"><span class="c0">User Flow 2: Not Applicable </span></p></td></tr><tr class="c6"><td class="c3" colspan="1" rowspan="1"><p class="c1 c10"><span class="c0"></span></p></td><td class="c14" colspan="1" rowspan="1"><p class="c1 c10"><span class="c0"></span></p></td><td class="c8" colspan="1" rowspan="1"><p class="c1 c10"><span class="c0"></span></p></td><td class="c8" colspan="1" rowspan="1"><p class="c1 c10"><span class="c0"></span></p></td><td class="c16" colspan="1" rowspan="1"><p class="c1 c10"><span class="c0"></span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c1 c10"><span class="c0"></span></p></td></tr></tbody></table>

<br></br>
