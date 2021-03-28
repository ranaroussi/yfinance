import utils
import numpy as np

# wrapping generic pattern functionality in function for testing
def generic_patterns(data):
    _financials = {
        "yearly": utils.empty_df(),
        "quarterly": utils.empty_df()}
    _balancesheet = {
        "yearly": utils.empty_df(),
        "quarterly": utils.empty_df()}
    _cashflow = {
        "yearly": utils.empty_df(),
        "quarterly": utils.empty_df()}
    for key in (
        (_cashflow, 'cashflowStatement', 'cashflowStatements'),
        (_balancesheet, 'balanceSheet', 'balanceSheetStatements'),
        (_financials, 'incomeStatement', 'incomeStatementHistory')
    ):
        item = key[1] + 'History'
        if isinstance(data.get(item), dict):
            try:
                key[0]['yearly'] = cleanup('yearly')
            except Exception as e:
                pass

        item = key[1]+'HistoryQuarterly'
        if isinstance(data.get(item), dict):
            try:
                key[0]['quarterly'] = cleanup('quarterly')
            except Exception as e:
                pass
    return data

# cleanup() function is not a part of the code from line 378 to 391
# therefore we won't be testing if cleanup works properly or not
def cleanup(howOften):
    done =  howOften + " cleaned up successfully"
    return done
