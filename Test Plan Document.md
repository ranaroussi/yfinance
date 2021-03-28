# Test Plan Document

## Table of Contents
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
- We will be testing the integrity of both yearly and quarterly balance sheet data retrieved from the yahoo finance website 

<a id="s2"><h4>1.2 Team Members</h4></a>

| Members  | Role |
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
We do the same tests for the quarterly balance sheets. Below are the tests we would run.

`def test_balance_sheet(self):
        for ticker in tickers:
            assert(ticker.balance_sheet is not None)
            assert(ticker.balance_sheet == ticker.get_balancesheet())

    def test_quarterly_balance_sheet(self):
        for ticker in tickers:
            assert(ticker.quarterly_balance_sheet is not None)
            assert(ticker.quarterly_balance_sheet == ticker.get_balancesheet(freq='quarterly'))

    def test_balancesheet(self):
        for ticker in tickers:
            assert(ticker.balancesheet is not None)
            assert(ticker.balancesheet == ticker.get_balancesheet())

    def test_quarterly_balancesheet(self):
        for ticker in tickers:
            assert(ticker.quarterly_balancesheet is not None)
            assert(ticker.quarterly_balancesheet == ticker.get_balancesheet('quarterly'))`


<a id="risks"><h2>4. Risks</h2></a>

| #  | Risk | Impact | Trigger | Mitigation Plan |
|---|---|---|---|---|
| 1 | Data frame is "None". Any attempt to access or parse the data could crash the application since it isn't expecting a "none" value | High |  | After getter method is called, perform a check to see if data frame is "None" before attempting to further parse the data. |
| 2 | Data frame passed into test is not equivalent to data frame from getter method | Medium |  | After getter method is called, check if the data is from the expected balance sheet or if it's from another balance sheet. |

<a id="testApproach"><h2>5. Test Approach</h2></a>
<a id="s5"><h4>5.1 Test Automation</h4></a>
- We will be implementing tests that can run via a python file. An attempt to retrieve data from the yahoo finance website must be made.
After the fetch is successful, tests can commence on “.balance_sheet” and “.quarterly_balance_sheet”. Both sheets are tested to see if their values are "None" or if they match the expected banacle sheet for the given trigger tested.

<a id="testEnvironment"><h2>6. Test Environment</h2></a>
- Everything is run locally on your machine. Data is retrieved from yahoo finance website.
























