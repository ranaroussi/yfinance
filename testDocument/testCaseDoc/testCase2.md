## System Test Case
**yfinance**<br><br>
**USER STORY:** Getting info on quarterly balance by calling ticker.quarterly_balance_sheet and ticker.quarterly_balancesheet

**Purpose:** Calling quarterly balance sheet and comapring it to expected quarterly balance sheet. We want to investigate the cause of empty quarterly balance sheet and incorrect quarterly balance sheet.<br>
**Tester Name(s):** Harshal Chhaniyara<br>
**Date(s) of Test:** March 28 2021<br><br>

**TEST SCRIPT STEPS/RESULTS**
| Step | Test Step/Input                                             | Expected Results                                         | Actual Results |
|------|-------------------------------------------------------------|----------------------------------------------------------|----------------|
| 1    | Navigate to the root directory and run Python               | Python shell runs                                        | pass           |
| 2.   | Run the test_yfinance.py                                    | all test run without any errors                          | pass           |
| 3.   | test case test_balance gets executed 	                     | test case gets runs without errors                       | pass           |
| 4.   | ticker.quarterly_balance_sheet gets called    	             | A dictionary and not None   		                        | pass           |
| 5.   | ticker.quarterly_balancesheet gets called 	                 | A dictionary and not None                                | pass           |
| 6.   | ticker.get_balance_sheet('quarterly') gets called 	         | quarterly_balance_sheet is same as the called dictonary  | pass           |
| 7.   | ticker.get_balancesheet('quarterly') gets called 	         | quarterly_balancesheet is same as the called dictonary   | pass           |