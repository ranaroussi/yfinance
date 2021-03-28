import unittest, utils
from data import generic_patterns, cleanup

class TestGenericPatterns(unittest.TestCase):
    # testing if data is a dict type
    def test_is_dict(self):
        empty_dict = dict()
        data = generic_patterns(empty_dict)
        self.assertTrue(isinstance(data, dict))

    # testing if loop is being handled properly
    def test_loop(self):
        data = {}
        alist = []
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
            alist.append(item)
        for i in range(len(alist)):
            if i == 0:
                self.assertTrue(alist[i] == "cashflowStatementHistory")
            elif i == 1:
                self.assertTrue(alist[i] == "balanceSheetHistory")
            elif i == 2:
                self.assertTrue(alist[i] == "incomeStatementHistory")
    
    # testing if item doesn't exist in the data dictionary
    def test_switch_statement(self):
        data = {}
        msg = ""
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
                    msg += cleanup('yearly')
                except Exception as e:
                    pass

            item = key[1]+'HistoryQuarterly'
            if isinstance(data.get(item), dict):
                try:
                    msg += cleanup('quarterly')
                except Exception as e:
                    pass
            
        if (not isinstance(data.get(item), dict)):
            msg += "Empty DataFrame"
        self.assertTrue(msg == "Empty DataFrame")

if __name__ == "__main__":
    unittest.main()