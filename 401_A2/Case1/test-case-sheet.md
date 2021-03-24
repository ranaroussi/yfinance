Test Case Sheet:

Test Run Information:

Tester Name: Shiyu Xiu

Date(s) of Test:2021/03/24

Prerequisites for this test: None

Operating System: MacOs Big Sur 11.2.3

Required Configuration: python 3.7 or up, numpy, pandas

Input:
Ticker ‘TSM’ Data that contains two different dividends and no splits.

Expected output:
Returned Dataframe object with correct dates as index and corresponding dividends amount as columns and empty splits Dataframe

Input:
Ticker ‘TSM’ Data that contains no different dividends and no splits events

Expected output:
Return two empty Dataframe
