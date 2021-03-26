# Test Plan

## Introduction
The Test Plan has been created to communicate the test approach to team members. It
includes the objectives, scope, schedule, risks and approach. This document will clearly
identify what the test deliverables will be and what is deemed in and out of scope.
### Objectives
yfinance is an open-source Python library used to download Yahoo! finance historical market data, acting in place of the original, decommissioned Yahoo! finance historical data API. This library uses Python Pandas for data manipulation.

For this plan, the test team will be responsible for testing the following two methods in `ticker.py` to ensure functionality: `.financials` and `.quarterly_financials`. Each test case, once implemented, will be accompanied with a Test Case Report Document. Issues and pull requests (both open and closed) related to the methods are to be explored and reported, especially if they suggest fixes identified by the team's tests. Lastly, potential fixes may be proposed to ensure tests pass (if any failed test cases exist). 

### Team Members
* Haochen Gou - Tester
* Pierre Hebert - Tester
* Mehrshad Sahebsara - Tester
* Eldon Lake - Tester
* Katherine Mae Patenio - Tester

| Name | Role |
|------|------|
| Haochen Gou | Tester |
| Pierre Hebert | Tester |
| Mehrshad Sahebsara | Tester |
| Eldon Lake | Tester |
| Katherine Mae Patenio | Tester |

## Assumptions / Risks
### Assumptions
This section lists assumptions that are made during testing.

1. `Ticker` module is fully implemented and can be used.
2. Any existing code that calls methods `.financials` and `.quarterly_financials` is fully implemented and works as intended.

### Risks
Risks and the appropriate actions to address them are identified in this section. The impact of a risk is based on how the project
would be affected if the risk was triggered. Trigger indicates an event that would
cause the risk to become an issue that needs to be resolved.

TODO - EXAMPLE BELOW

| # | Risk | Impact | Trigger | Mitigation Plan |
|---|------|--------|---------|-----------------|
| 1 | Scope Creep – as testers become more familiar with the tool, they will want more functionality | High | Delays in implementation on date | Each iteration, functionality will be closely monitored. Priorities will be set and discussed by stakeholders. Since the driver is functionality and not time, it may be necessary to push the date out.
| 2 | Changes to the functionality may negate the tests already written and we may loose test cases already written. | High - to schedule and quality | Loss of a test cases | Export data prior to any upgrade, massage as necessary and re-inport after upgrade.
| 3 | Weekly delivery is not possible because developer works off site | Medium | Product did not get delivered on schedule