# System Test Case - Yahoo! Finance market data downloader
### Ticker.financials property: positive test case
##### **Purpose:** 
Verify that the financials property successfully returns the financials dataframe from the ticker object.  
##### **Test Run Information:**  
Tester name: Eldon Lake  
Date of Test: March 27, 2021  
##### **Prerequisites for this test:**
Install dill 0.3.3 for python: pip install dill  
##### **Software versions for this test:**  
OS: Windows 10 version 10.0.19041 Build 19041  
Python: 3.9.1  
All modules as specified in requirements.txt  

##### **Notes:**
* Validates the financials(self) property at line 156 of ticker.py
* This property is a one-line reference to the self.get_financials() getter
* In order to test this getter independently, the following isolation steps were taken:
  1. A Ticker object is loaded from a binary file
  2. A flag is added to get_financials to bypass calling get_fundamentals()
* These isolations steps were necessary because:
  1. The Ticker constructor makes an http request to an external API, which can produce uncontrollable behaviour
  2. The get_fundamentals() method makes http requests to an external API, which can produce uncontrollable behaviour

##### **Test Script:**
1. Ensure that you have installed all of the python modules listed in requirements.txt
2. Additionally, you must install dill. If you are using pip: pip install dill
3. Open a new terminal and cd to the root yfinance directory
4. Verify that the mfst.dill file is present in this directory
5. Run the tests with the following command: python -m yfinance.ticker -v
6. Inspect the output to confirm that the final line of the output reads "Test passed."
7. If there are any failures, inspecting the preceding lines of the output to see further details.

##### **Results:**

**Output:**  
1 items passed all tests:  
   5 tests in __main__.Ticker.financials  
5 tests in 28 items.  
5 passed and 0 failed.  
Test passed.  
**Interpretation:**  
This indicates that all 5 statements in our test have passed, meaning the overall test has passed. This confirms that our property is successfully mapped to the correct getter, and that the getter is returning the correct values from our mocked data.