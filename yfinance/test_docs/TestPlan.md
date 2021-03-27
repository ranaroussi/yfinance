# Assignment 2 Test Plan Document

## yfinance Group #5 (line 378 to 391)
**Ji Heon Kim**†<br>
&nbsp;&nbsp;&nbsp;&nbsp;Department of Computer Science, University of Alberta, Edmonton, Alberta, Canada, jiheon@ualberta.ca<br>
**Chirag Khurana**†<br>
&nbsp;&nbsp;&nbsp;&nbsp;Department of Computer Science, University of Alberta, Edmonton, Alberta, Canada, ckhurana@ualberta.ca <br>
**Christian Arbelaez**†<br>
&nbsp;&nbsp;&nbsp;&nbsp;Department of Computer Science, University of Alberta, Edmonton, Alberta, Canada, carbelae@ualberta.ca <br>
**Scott Kavalinas**†<br>
&nbsp;&nbsp;&nbsp;&nbsp;Department of Computer Science, University of Alberta, Edmonton, Alberta, Canada, skavalin@ualberta.ca <br>
**Walker Peters**†<br>
&nbsp;&nbsp;&nbsp;&nbsp;Department of Computer Science, University of Alberta, Edmonton, Alberta, Canada, wpeters@ualberta.ca <br>

*† Undergraduate student.*


## Introduction
**1.1 Objectives**<br>
yfinance is a library, developed by yahoo, that provides a Pythonic way to download historical market data.<br>
Our objective for this assignment is to isolate a part of a big chunk of code and run functional tests to simulate 
multiple runs with different values and to see if the output matches the desired output.

**1.2 Team Members**<br>
| Name               | Role |
|--------------------|------|
| Chirag Khurana     | N/A  |
| Christian Arbelaez | N/A  |
| Ji Heon Kim        | N/A  |
| Scott Kavalinas    | N/A  |
| Walker Peters      | N/A  |


## Research and Screening
**2.1 Issues related to the code**<br>
- GitHub Issue #599 (https://github.com/ranaroussi/yfinance/issues/599)
- GitHub Issue #144 (https://github.com/ranaroussi/yfinance/issues/144)

**2.2 Pull requests related to the code**<br>
- Pull Request #497 (https://github.com/ranaroussi/yfinance/pull/497)


## Assumptions / Risks
**3.1 Assumptions**<br>
- We assume that all the keys and values in the dictionaries are string type. This is a fair assumption to make 
since we do not have access to yahoo’s data, which means that we need to work with mock-up data. As long as the 
input data types match the data types of what it’s comparing, the test should pass successfully
- We assume that the functions being used in our part of the code work fine with string type as the dictionary 
values. This assumption needs to be made if we assume all the dictionary values to be string type.

**3.2 Risks**<br>
- Working with only string type may result in a mismatch between the test output and the actual output.


## Test Approach
**4.1 Testing Method**<br>
The part of the code we are assigned to is a loop that iterates through a dictionary, finds specific keys, and 
runs a cleanup operation to the data if the found keys are of a specific type. A good test approach for us would 
be exploratory testing. A reason for that is because we do not have the actual data to test with. We will have to 
create dummy data to work with and test using dummy data for specific testing scenarios. 

**4.2 Test Automation**<br>
Automated functional testing is going to be tested, since functional testing is performed at the level of system testing.


## Test Environment
The database with yahoo's data would be required for testing actual values, however dummy data can be used for the sake of testing functionalities.

## Milestone / Deliverables
**6.1 Test Schedule**<br>
| Task Name          | Start  | Finish | Effort | Comments |
|--------------------|--------|--------|--------|----------|
| Test Planning      | Mar 27 | Mar 27 | 0 d    | N/A      |
| N/A                | N/A    | N/A    | 0 d    | N/A      |
| N/A                | N/A    | N/A    | 0 d    | N/A      |
| N/A                | N/A    | N/A    | 0 d    | N/A      |
| N/A                | N/A    | N/A    | 0 d    | N/A      |
