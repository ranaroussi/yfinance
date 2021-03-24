def info_func(data):  
    self._info = {}
    items = ['summaryProfile', 'summaryDetail', 'quoteType',
              'defaultKeyStatistics', 'assetProfile', 'summaryDetail', 'financialData']
    for item in items:
        if isinstance(data.get(item), dict):
            self._info.update(data[item])

    self._info['regularMarketPrice'] = self._info['regularMarketOpen']
    self._info['logo_url'] = ""
    try:
        domain = self._info['website'].split(
            '://')[1].split('/')[0].replace('www.', '')
        self._info['logo_url'] = 'https://logo.clearbit.com/%s' % domain
    except Exception:
        pass
    
    return self._info