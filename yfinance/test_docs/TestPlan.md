# Assignment 2 Test Plan Document

## yfinance Group #5 (line 378 to 391)
**Ji Heon Kim**†<br>
&nbsp;&nbsp;&nbsp;&nbsp;Department of Computer Science, University of Alberta, Edmonton, Alberta, Canada, jiheon@ualberta.ca<br>
**Chirag Khurana**†<br>
&nbsp;&nbsp;&nbsp;&nbsp;Department of Computer Science, University of Alberta, Edmonton, Alberta, Canada, ckhurana@ualberta.ca <br>
**Christian Arbelaez**†<br>
&nbsp;&nbsp;&nbsp;&nbsp;Department of Computer Science, University of Alberta, Edmonton, Alberta, Canada, carbelae@ualberta.c<br>
**Scott Kavalinas**†<br>
&nbsp;&nbsp;&nbsp;&nbsp;Department of Computer Science, University of Alberta, Edmonton, Alberta, Canada, skavalin@ualberta.ca <br>
**Walker Peters**†<br>
&nbsp;&nbsp;&nbsp;&nbsp;Department of Computer Science, University of Alberta, Edmonton, Alberta, Canada, wpeters@ualberta.ca <br>

*† Undergraduate student.*


## Introduction
**1.1 Objectives**<br>
yfinance is a library, developed by yahoo, that provides a Pythonic way to download historical market data.<br>
Our objective for this assignment is to isolate a part of a big chunk of code and run functional tests to simulate 
multiple runs with different values and to see if the output matches the desired output. Specifically, we want to 
isolate the code tagged under `# generic patterns` in the `_get_fundamentals` function of *base.py*. Our goal is
to thoroughly test the functionality contained in this section of code and uncover any bugs/improvements we can.

**1.2 Team Members**<br>
| Name               | Role     |
|--------------------|----------|
| Ji Heon Kim        | Testing  |
| Chirag Khura       | Testing  |
| Christian Arbelaez | Testing  |
| Scott Kavalinas    | Testing  |
| Walker Peters      | Testing  |


## Research and Screening
**2.1 Issues related to the code**<br>
**Open**
- GitHub Issue #599 (https://github.com/ranaroussi/yfinance/issues/599)
- GitHub Issue #172 (https://github.com/ranaroussi/yfinance/issues/172)
- GitHub Issue #167 (https://github.com/ranaroussi/yfinance/issues/167)
- GitHub Issue #163 (https://github.com/ranaroussi/yfinance/issues/163)
- GitHub Issue #150 (https://github.com/ranaroussi/yfinance/issues/150)

**Closed**
- GitHub Issue #275 (https://github.com/ranaroussi/yfinance/issues/275)
- GitHub Issue #142 (https://github.com/ranaroussi/yfinance/issues/142)
- GitHub Issue #140 (https://github.com/ranaroussi/yfinance/issues/140)

**2.2 Pull requests related to the code**<br>
**Closed**
- Pull Request #497 (https://github.com/ranaroussi/yfinance/pull/497)
- Pull Request #174 (https://github.com/ranaroussi/yfinance/pull/174)


## Assumptions / Risks
**3.1 Assumptions**<br>
- We assume that all the keys and values in the dictionaries are string type. This is a fair assumption to make 
since we do not have access to yahoo’s data, which means that we need to work with mock-up data. As long as the 
input data types match the data types of what it’s comparing, the test should pass successfully
- We assume that the functions being used in our part of the code work fine with string type as the dictionary 
values. This assumption needs to be made if we assume all the dictionary values to be string type.

**3.2 Risks**<br>
| Risk                                                                                                              | Impact | Trigger                                         | Mitigation Plan                                                                    |
|-------------------------------------------------------------------------------------------------------------------|--------|-------------------------------------------------|------------------------------------------------------------------------------------|
| Scope Creep - as testers start to get further along with testing, we realize there is more to do than anticipated | High   | Testing implementation proves to be difficult   | Start working on the testing as early as possible.                                 |
| Working with only string type may result in a mismatch  between the test output and the actual output             | Medium | Mismatching types causes testing to take longer | Be mindful  of this fact                                                           |
| Familiarity with testing in Python causes  testing to take longer than expected                                   | Medium | Delays in implementing required testing         | Read Python testing documentation. Spend a fair amount of time working on project. |


## Test Approach
**4.1 Testing Method**<br>
The part of the code we are assigned to is a loop that iterates through a dictionary, finds specific keys, and 
runs a cleanup operation to the data if the found keys are of a specific type. A good test approach for us would 
be exploratory testing. A reason for that is because we do not have the actual data to test with. We will have to 
create mock data to work with and test using mock data for specific testing scenarios. 

**4.2 Test Automation**<br>
Automated functional testing is going to be tested, since functional testing is performed at the level of system testing. 
Automated unit tests are not a part of the process and testing is currently done manually using Python's Unit testing 
framework, unittest (https://docs.python.org/3.9/library/unittest.html).

## Test Environment
The database with yahoo's data would be required for testing actual values, however mock data can be used for the sake of testing 
functionalities. Python is required to run the code contained in this project, and you can install the required modules using pip by 
typing `pip3 install -r requirements.txt` or `pip install -r requirements.txt` (depending on your configuration) in the root directory.

## Milestone / Deliverables
**6.1 Test Schedule**<br>
| Task Name                 | Start  | Finish | Effort | Comments |
|---------------------------|--------|--------|--------|----------|
| Test Planning             | Mar 25 | Mar 25 | 0d     | N/A      |
| Documentating             | Mar 25 | Mar 27 | 2d     | N/A      |
| Testing Functionality     | Mar 25 | Mar 27 | 2d     | N/A      |
| Testing Data Type         | Mar 25 | Mar 27 | 2d     | N/A      |
| Testing Dictionary Values | Mar 25 | Mar 27 | 2d     | N/A      |

**6.2 Deliverables**<br>
| Deliverable        | For                | Date / Milestone |
|--------------------|--------------------|------------------|
| Test Plan          | All team members   | Mar 25           |
| Test Results       | All team members   | Mar 27           |
| Test Status Report | All team members   | Mar 27           |
| Metrics            | All team members   | Mar 27           |
