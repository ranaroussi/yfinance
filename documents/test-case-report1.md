**<yfinance>**

**Test Case Report**

**<03/28/21>**

**TEST CASE REPORT**

**GENERAL INFORMATION**

**Test Stage:**

- [ ] Performance 
- [x] Functionality 
- [ ] Regression 
- [ ] Integration 
- [x] Acceptance 
- [ ] System 
- [ ] Pilot 
- [ ] Interface

**Test Date:** 03/27/21

**System Date, if applicable:** N/A

**Tester:** Harshal, Jonathan, Jordon, JuSong, Ravi

**Test Case Number:** #657

**Test Case Description** Testing balancesheet and balance_sheet to ensure that they are returning something.

**Results:**

- [x] Pass 
- [ ] Fail

**Incident Number, if applicable:** #618, #595, #547, #191, #465, #474, #475, #419, #423

**INTRODUCTION**

**Requirement(s) to be tested:** python, terminal

**Roles and Responsibilities:** Every team member ran the tests on their end to ensure consistency between results.

**Set Up Procedures:** Nvigate to main directory, run test_yfinance.py with command python -m test_yfinance.py.

**Stop Procedures:** Testing should autoend.

**ENVIRONMENTAL NEEDS**

**Hardware:** A device capable of handling the minimum requirements of python3.

**Software:** unittest framework for python3.

**Procedural Requirements:** None

**TEST**

**Test Items and Features:** balancesheet: a table that contains information on balances. Need to ensure that the balancesheet is not empty upon returning.

**Input Specifications:** N/A

**Procedural Steps:** Have all the data related to balancesheet setup and ready to go, then execute the test for balancesheet. If everything is working and balancesheet contains information, the testcases will pass. However, if the balancesheet is empty, the testcase will fail.

**Expected Results of Case:** The outcome anticipated is pass if data on balancesheet is provided. The item will pass the test as long as balancesheet is not None.

**ACTUAL RESULTS**

**Output Specifications:** 
**TEST SCRIPT STEPS/RESULTS**

| Step | Test Step/Input                                             | Expected Results                               | Actual Results |
|------|-------------------------------------------------------------|------------------------------------------------|----------------|
| 1    | Navigate to the root directory and run Python               | Python shell runs                              | pass           |
| 2.   | Run the test_yfinance.py                                    | all test run without any errors                | pass           |
| 3.   | test case test_balance gets executed 	                     | test case gets runs without errors             | pass           |
| 4.   | ticker.balance_sheet gets called    	                     | A dictionary and not None   		              | pass           |
| 5.   | ticker.balancesheet gets called 	                     	 | A dictionary and not None                      | pass           |
| 6.   | ticker.get_balance_sheet() gets called 	                 | balance_sheet is same as the called dictonary  | pass           |
| 7.   | ticker.get_balancesheet() gets called 	                     | balancesheet is same as the called dictonary   | pass           |


