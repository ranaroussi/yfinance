# Test Plan Document

## Shortcut
* [1. Introduction](#intro)
  - [1.1 Objective](#s1)
  - [1.2 Team Members](#s2)
* [2. Problems](#problems)
  - [2.1 Issues](#issues)
  - [2.2 Pull Requests](#pr)
  - [2.3 Problems found](#pf)
* [3. Planned Tests](#plannedTests)
* [4. Risks](#risks)
* [5. Test Approach](#testApproach)
  - [5.1 Test Automation](#s5)
* [6. Test Environment](#testEnvironment)

***

<a id="intro"><h2>1 Introduction</h2></a>
<a id="s1"><h4>1.1 Objectives</h4></a>
- Yahoo! Finance market data downloader is designed to make it easy for users to download any historic market data from Yahoo! Finance website using python scripts.

<a id="s2"><h4>1.2 Team Members</h4></a>

| Member  | Role |
|---|---|
|  Ravi, Jordon |  Test Plan Document |
| Justong, Harshal  |  Test Case Document |
|  Alquaryu |  Test Report Document |


<a id="problems"><h2>2. Problems</h2></a>

<a id="issues"><h4>2.1 Issues</h4></a>
- Issues #618, #595, #547, #191, #465, #474, #475, #419, #423

<a id="pr"><h4>2.2 Pull Requests</h4></a>
- Pull requests #480, #526

<a id="pf"><h4>2.3 Problems Found</h4></a>
- When attempting to retrieve a balance sheet, an empty data frame is returned.

<a id="plannedTests"><h2>3. Planned Tests</h2></a>
- We plan to do two tests cases for each of the four methods we need to test. For each method we test whether the balance sheet is None, 
and we test if the balance sheet passed into the test is equivalent to what we receive from "ticker.get_balancesheet()". 
We do the same tests for the quarterly balance sheets.

<a id="risks"><h2>4. Risks</h2></a>

| #  | Risk | Impact | Trigger | Mitigation Plan |
|---|---|---|---|---|
| 1 |  |  |  | |  |  |
| 2 |  |  |  | |  |  |
| 3 |  |  |  | |  |  |
| 4 |  |  |  | |  |  |

<a id="testApproach"><h2>5. Test Approach</h2></a>
<a id="s5"><h4>5.1 Test Automation</h4></a>
- We will be implementing tests that can be run via a python file. An attempt to retrieve data from the yahoo finance website must be made.
After data is successfully retrieved and parsed, tests can commence on “.balance_sheet” and “.quarterly_balance_sheet”.

<a id="testEnvironment"><h2>6. Test Environment</h2></a>
- Everything is run locally on your machine. Data is retrieved from yahoo finance website.
























