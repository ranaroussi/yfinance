TEST PLAN

# Introduction
## Objective
The objective of this assignment is to improve our abilities of reading other people’s code. We also hope to learn new testing strategies and how to document them. 
Another objective is to get exposed to the Open Source projects and learn the process of how to contribute to them properly and in the best way possible. 

## Team Members
| Member Names        | Role   |
|:-------------------:|:------:|
| Anastasia Borissova | Tester |
| Khang Nguyen        | Tester |
|                     | Tester |
|                     | Tester |
|                     | Tester |

## Research
Issues:
1. **No "calendar" attribute in yf.Ticker object #562.** It appears that the the .calendar attribute is not returning the next earnings event: AttributeError: 'Ticker' object has no attribute 'calendar' Tried a few different tickers, but it does not work with "MSFT", the example ticker

## Planned Tests
1. check if the variable ‘cal’ gets an empty pandas DataFrame, in other words, check if the variable 'cal' is not receiving proper data.
2. check if the code properly handles when the variable ‘cal’ gets a pandas DataFrame that contains only NaNs.

## Risks

## Test Approach
Read and understand the code assigned to us, and develop tests that address the functionality of the code block. No automation.

## Test Environment
A new server is required for the web server, the application and the database. 

## Test Deliverables
