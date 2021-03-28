# Test Case Report

## General Infromation
Test Stage: Unit Testing

Test Date: Mar 27, 2021

Testers: Spencer von der Ohe

Test Case Description: Testing negative, positive, zero, quarterly negative, quarterly positive, quarterly zero earnings requests for the .earnings methods of the ticker.py for the yfinance module.

Test Case Number: 6

Results: Passed all tests

## Introduction

Requirement(s) to be tested: .earnings 

Roles and Responsibilities: The team's goal was to test for the proper response from the .earnings method for multiple different valid and invalid inputs.

Set Up Procedures:<br>
Retrieve codebase<br>
*Assuming user has github account and git installed onto their machine*<br>
1. `git clone https://github.com/sazzy4o/yfinance.git`
Setup Virtual Environment (recommended)<br>
3. Install venv for Python
4. Create virtual environment
`virtualenv <desired path>`
4. Activate virtual environment (macOS)
`source <desired path/bin/activate>`
Install Necessary Modules<br>
5. `pip3 install unittest`
6. `pip3 install . -editable`

Stop Procedures: Press `CTRL + C`

## Environmental Needs
Hardware: Any computer capable of running a modern operating system.

Software: Any modern operating system capable of running python3.

Procedural Requirements:<br>
Ensure the “data” folder contains the correct json files. Must run `pip install . -editable` inside the yfinance folder in order to install yfinance’s required additional modules.

## Test
Test Items: 

Test Features:

Input Specifications: N/A, all tests run automatically

Procedural Steps:<br>
1. Go to test directory `cd test`
2. run command `python3 test_earnings.py`
The test case is run with the unittest module. This test case is set up under the “TestDataValues” class, and the tests themselves are all run using the python code: `unittest.main()`, which is run when the file “test_earnings.py” is run.

Expected Results of Case:<br>
The output will say that a test failed if a test failed. It will show the following if the test is completed successfully:

`.....`
`---------------------------------------------------`
`Ran 5 tests in 9.537s`
`OK`

## Actual Results
Output Specifications:
