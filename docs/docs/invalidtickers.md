Test Case: Invalid values

Purpose: Prevent errors with:



    1. Invalid ticketer
    2. Invalid company

Test Run Information: 

Tester Names: Cameron and Ryan

Date(s) of Test:

Prerequisites for this test: None

Software Versions: Current version of yfinance

Required Configuration:

Notes and Results:


<table>
  <tr>
   <td>Step
   </td>
   <td>Test Step/Input
   </td>
   <td>Expected Results
   </td>
   <td>Actual Results
   </td>
   <td>Requirements Validation
   </td>
   <td>Pass/Fail
   </td>
  </tr>
  <tr>
   <td colspan="6" >User Flow 1: Testing empty string
   </td>
  </tr>
  <tr>
   <td>Create Ticker
   </td>
   <td>Ticker(‘’)
   </td>
   <td>Ticker created
   </td>
   <td>Ticker created
   </td>
   <td>
   </td>
   <td>Pass
   </td>
  </tr>
  <tr>
   <td>Get earnings
   </td>
   <td>Ticker.earnings
   </td>
   <td>Return error
   </td>
   <td>HTTPError
   </td>
   <td>
   </td>
   <td>Pass
   </td>
  </tr>
  <tr>
   <td colspan="6" >User Flow 2: Testing debunk company
   </td>
  </tr>
  <tr>
   <td>Create Ticker
   </td>
   <td>Ticker(‘LEHLQ’)
   </td>
   <td>Ticker created
   </td>
   <td>Ticker created
   </td>
   <td>
   </td>
   <td>Pass
   </td>
  </tr>
  <tr>
   <td>Get earnings
   </td>
   <td>Ticker.earnings
   </td>
   <td>Return error
   </td>
   <td>KeyError
   </td>
   <td>
   </td>
   <td>Pass
   </td>
  </tr>
  <tr>
   <td colspan="6" >User Flow 3: Misspelled company (GOOE vs GOOG)
   </td>
  </tr>
  <tr>
   <td>Create Ticker
   </td>
   <td>Ticker(‘GOOE’)
   </td>
   <td>Ticker created
   </td>
   <td>Ticker created
   </td>
   <td>
   </td>
   <td>Pass
   </td>
  </tr>
  <tr>
   <td>Get earnings
   </td>
   <td>Ticker.earnings
   </td>
   <td>Return error
   </td>
   <td>KeyError
   </td>
   <td>
   </td>
   <td>Pass
   </td>
  </tr>
  <tr>
   <td colspan="6" >User Flow 4: White Space
   </td>
  </tr>
  <tr>
   <td>Create Ticker
   </td>
   <td>Ticker(‘ ’)
   </td>
   <td>Ticker created
   </td>
   <td>Ticker created
   </td>
   <td>
   </td>
   <td>Pass
   </td>
  </tr>
  <tr>
   <td>Get earnings
   </td>
   <td>Ticker.earnings
   </td>
   <td>Return error
   </td>
   <td>InvalidURL
   </td>
   <td>
   </td>
   <td>Pass
   </td>
  </tr>
  <tr>
   <td colspan="6" >User Flow 5: Numbers
   </td>
  </tr>
  <tr>
   <td>Create Ticker
   </td>
   <td>Ticker(‘123’)
   </td>
   <td>Ticker created
   </td>
   <td>Ticker created
   </td>
   <td>
   </td>
   <td>Pass
   </td>
  </tr>
  <tr>
   <td>Get earnings
   </td>
   <td>Ticker.earnings
   </td>
   <td>Return error
   </td>
   <td>KeyError
   </td>
   <td>
   </td>
   <td>Pass
   </td>
  </tr>
</table>

