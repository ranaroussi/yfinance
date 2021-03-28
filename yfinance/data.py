import utils
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
                key[0]['yearly'] = cleanup(data[item][key[2]])
            except Exception as e:
                pass

        item = key[1]+'HistoryQuarterly'
        if isinstance(data.get(item), dict):
            try:
                key[0]['quarterly'] = cleanup(data[item][key[2]])
            except Exception as e:
                pass
    return data

def cleanup(data):
    df = _pd.DataFrame(data).drop(columns=['maxAge'])
    for col in df.columns:
        df[col] = _np.where(
            df[col].astype(str) == '-', _np.nan, df[col])

    df.set_index('endDate', inplace=True)
    try:
        df.index = _pd.to_datetime(df.index, unit='s')
    except ValueError:
        df.index = _pd.to_datetime(df.index)
    df = df.T
    df.columns.name = ''
    df.index.name = 'Breakdown'

    df.index = utils.camel2title(df.index)
    return df
