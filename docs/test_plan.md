# Test Plan

## Introduction
This Test Plan was created to communicate how team members will approach testing. It
includes objectives, research findings, planned tests, assumptions, and risks.
### Objectives
yfinance is an open-source Python library used to download Yahoo! finance historical market data, acting in place of the original, decommissioned Yahoo! finance historical data API. This library uses Python Pandas for data manipulation.

For this plan, the test team will be responsible for testing the following two methods in `ticker.py` to ensure functionality: `.financials` and `.quarterly_financials`. Each test case, once implemented, will be accompanied with a Test Case Report Document. Team will also conduct research, finding and reporting issues and pull requests (both open and closed) related to the methods, especially if they suggest fixes identified by the team's tests. Lastly, potential fixes may be proposed to ensure tests pass (if any failed test cases exist). 

### Team Members
| Name | Role |
|------|------|
| Haochen Gou | Tester |
| Pierre Hebert | Tester |
| Mehrshad Sahebsara | Tester |
| Eldon Lake | Tester |
| Katherine Mae Patenio | Tester |

## Research
### GitHub Issues
Several open and closed issues related to the two methods `.financials` and `.quarterly_financials` were found. We will use our findings to determine our testing approach.

#### Closed Issues
Most of the closed issues were resolved due to features or fixes being implemented from other pull requests. Some were closed due to lack of updates. One issue in particular requested that `.quarterly_financials` be added as a feature.
- [#393](https://github.com/ranaroussi/yfinance/issues/393)
- [#547](https://github.com/ranaroussi/yfinance/issues/547)
- [#386](https://github.com/ranaroussi/yfinance/issues/386)
- [#350](https://github.com/ranaroussi/yfinance/issues/350)
- [#142](https://github.com/ranaroussi/yfinance/issues/142)
- [#111](https://github.com/ranaroussi/yfinance/issues/111)
- [#90](https://github.com/ranaroussi/yfinance/issues/90)
- [#158](https://github.com/ranaroussi/yfinance/issues/158)

#### Open Issues
Most of the open issues focus on unexpected or buggy behaviour. Some were found to be simply feature requests or potential workarounds relating to `.financials` and `.quarterly_financials`. It is possible that some issues were already addressed, but have not yet been closed.
- [#639](https://github.com/ranaroussi/yfinance/issues/639)
- [#618](https://github.com/ranaroussi/yfinance/issues/618)
- [#519](https://github.com/ranaroussi/yfinance/issues/519)
- [#518](https://github.com/ranaroussi/yfinance/issues/518)
- [#595](https://github.com/ranaroussi/yfinance/issues/595)
- [#593](https://github.com/ranaroussi/yfinance/issues/593) 
- [#582](https://github.com/ranaroussi/yfinance/issues/582)
- [#561](https://github.com/ranaroussi/yfinance/issues/561)
- [#533](https://github.com/ranaroussi/yfinance/issues/533)
- [#476](https://github.com/ranaroussi/yfinance/issues/476)
- [#471](https://github.com/ranaroussi/yfinance/issues/471)
- [#144](https://github.com/ranaroussi/yfinance/issues/144)
- [#158](https://github.com/ranaroussi/yfinance/issues/158)
- [#191](https://github.com/ranaroussi/yfinance/issues/191)
- [#193](https://github.com/ranaroussi/yfinance/issues/193)
- [#214](https://github.com/ranaroussi/yfinance/issues/214)
- [#240](https://github.com/ranaroussi/yfinance/issues/240)
- [#271](https://github.com/ranaroussi/yfinance/issues/271)

### Pull Requests
Pull requests provide fixes or extra functionality. 

#### Open Pull Requests
No open pull requests relevant to the two methods were found.

#### Closed Pull Requests
Some pull requests were merged to integrate new fixes or changes, whilst others were closed due to being duplicates of already merged in pull requests - such as [#480](https://github.com/ranaroussi/yfinance/pull/480).

- [#534](https://github.com/ranaroussi/yfinance/pull/534)
- [#480](https://github.com/ranaroussi/yfinance/pull/480)
- [#104](https://github.com/ranaroussi/yfinance/pull/104)
- [#488](https://github.com/ranaroussi/yfinance/pull/488) - closed due to #480
- [#392](https://github.com/ranaroussi/yfinance/pull/392) - closed due to #480
- [#517](https://github.com/ranaroussi/yfinance/pull/517) - closed due to #480
- [#321](https://github.com/ranaroussi/yfinance/pull/321) - closed due to #480
- [#246](https://github.com/ranaroussi/yfinance/pull/246) - closed due to #480
- [#188](https://github.com/ranaroussi/yfinance/pull/188) - closed due to #480
- [#161](https://github.com/ranaroussi/yfinance/pull/161) - closed due to #480
- [#160](https://github.com/ranaroussi/yfinance/pull/160) - closed due to #480
- [#534](https://github.com/ranaroussi/yfinance/pull/534) - closed due to #480

### Overall Findings
Fixes were previously made to fix issues due to empty output with `.financials`. Previous contributions showed discussions made prior to integrating `.quarterly_financials` into the main codebase. However, none addressed unit tests for `.financials` and `.quarterly_financials`.

## Test Approach
Since yfinance is an open-source project, many collaborators contribute to the development of this project by means of issues and pull requests. The test team is to proceed with implementing test cases, based on our findings from relevant issues and pull requests, whilst keeping up to date with changes made to the the two methods `.financials` and `.quarterly_financials`.

Since yfinance is also a new tool to the test team, exploratory testing will be employed; essentially, the team will learn as they go.

### Test Environment
Existing unit test modules such as `unittest` will be used to create new unit test cases, as well as mock any data needed for each case.

### Planned Tests
In total, the team plans to implement 4 test cases - two for each method.

#### `.financials`
- assert that output is accurate when a valid (mock) input is received
- assert throws exception when invalid (mock) input is received

#### `.quarterly-financials`
- assert that output is accurate when a valid (mock) input is received
- assert throws exception when invalid (mock) input is received

## Assumptions / Risks
### Assumptions
This section lists assumptions that are made during testing.

1. `Ticker` module is fully implemented and can be used.
2. Any existing code that calls methods `.financials` and `.quarterly_financials` is fully implemented.

### Risks
Risks and the appropriate actions to address them are identified in this section. The impact of a risk is based on how the project
would be affected if the risk was triggered. Trigger indicates an event that would
cause the risk to become an issue that needs to be resolved.

| # | Risk | Impact | Trigger | Mitigation Plan |
|---|------|--------|---------|-----------------|
| 1 | Changes to functionallity for the methods in question may render already implemented test cases obsolete. | High - loss of quality assurance | Loss of test cases | Communicate to contributers that unit tests must pass or be updated prior to submitting pull requests.
| 2 | Changes to functionality for the methods in question may cause already implemented test cases to fail. | High - delays in schedule | Test cases fail |