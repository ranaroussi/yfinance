# 3. Processo de Refatoração

Refatoração concluída sobre o pacote `yfinance` v1.4.1. Baseline em `metrics-before-pylint/pylint_refactor_antes.json` (R0915=12, R0902=11, R0912=18). Resultado final em `metrics-after-pylint/pylint_refactor_depois.json` (todos zero).

---

## 3.1 too-many-statements (R0915)

### 3.1.1 `yfinance/screener/screener.py` → `screen()`

**Problema:** Função monolítica com dezenas de statements misturando validação, montagem de query, chamadas HTTP e normalização de resposta.

**Solução:** Extract Method — 8 helpers privados (`_validate_screen_limits`, `_resolve_predefined_with_offset`, `_fetch_predefined_screen`, `_build_custom_post_query`, etc.). `screen()` virou orquestrador com fluxo linear.

**Antes (trecho):**
```python
def screen(query, offset=None, size=None, count=None, sortField=None, sortAsc=None, ...):
    # validação inline, branches para predefined vs custom,
    # montagem de params, POST, parsing — tudo no mesmo corpo (~80+ statements)
    ...
```

**Depois (trecho):**
```python
def screen(query, ...):
    defaults = _screen_defaults()
    _validate_screen_limits(count, size)
    params_dict = _build_screen_params()
    if isinstance(query, str):
        return _fetch_predefined_screen(data, query, fields, params_dict, size)
    post_query = _build_custom_post_query(query, fields, defaults)
    return _post_custom_screen(data, post_query, params_dict)
```

---

### 3.1.2 `yfinance/multi.py` → `_download_impl()`

**Problema:** Download multi-ticker concentrava logging, resolução ISIN, execução threaded/sequential, tratamento de erros e concatenação de DataFrames num único método.

**Solução:** Fases extraídas em helpers: `_configure_download_logging`, `_resolve_download_tickers`, `_run_threaded_downloads`, `_run_sequential_downloads`, `_log_download_failures`, `_assemble_download_dataframe`.

**Antes (trecho):**
```python
def _download_impl(ctx, tickers, ...):
    # ~70+ statements: session, ISIN loop, threading, progress bar,
    # error aggregation, concat, multi-index — tudo inline
    ...
```

**Depois (trecho):**
```python
def _download_impl(ctx, tickers, ...):
    threads, progress = _configure_download_logging(logger, threads, progress)
    tickers = _resolve_download_tickers(ctx, tickers)
    if threads:
        _run_threaded_downloads(ctx, tickers, threads, progress, download_kwargs)
    else:
        _run_sequential_downloads(ctx, tickers, progress, download_kwargs)
    _log_download_failures(ctx, logger)
    return _assemble_download_dataframe(ctx, ignore_tz, group_by, multi_level_index, tickers)
```

---

## 3.2 too-many-instance-attributes (R0902)

### 3.2.1 `yfinance/search.py` → `Search`

**Problema:** 15+ atributos de instância no `__init__` (parâmetros de busca, estado de resposta, logger).

**Solução:** Agrupamento em dois dicts internos `_params` e `_results`. Propriedades públicas (`quotes`, `news`, `all`, etc.) delegam a `_results` — API inalterada.

**Antes (trecho):**
```python
class Search:
    def __init__(self, query, max_results=500, ...):
        self.query = query
        self.max_results = max_results
        self.news_count = news_count
        # ... mais 12 atributos self._* espalhados
```

**Depois (trecho):**
```python
class Search:
    def __init__(self, query, max_results=8, ...):
        self._params = {'query': query, 'max_results': max_results, ...}
        self._results = {'response': None, 'quotes': None, 'news': None, ...}

    @property
    def quotes(self):
        return self._results['quotes']
```

---

### 3.2.2 `yfinance/scrapers/analysis.py` → `Analysis`

**Problema:** Classe scraper com um `self._*` por cache lazy (`earnings_trend`, `analyst_price_targets`, etc.), totalizando >7 atributos.

**Solução:** Dict único `self._cache` com chaves nomeadas; métodos acessam `self._cache['earnings_trend']` em vez de atributos soltos.

**Antes (trecho):**
```python
class Analysis:
    def __init__(self, data, symbol):
        self._data = data
        self._symbol = symbol
        self._earnings_trend = None
        self._analyst_price_targets = None
        self._earnings_estimate = None
        # ... mais 5 caches
```

**Depois (trecho):**
```python
class Analysis:
    def __init__(self, data, symbol):
        self._data = data
        self._symbol = symbol
        self._cache = {k: None for k in (
            'earnings_trend', 'analyst_price_targets', 'earnings_estimate',
            'revenue_estimate', 'earnings_history', 'eps_trend', 'eps_revisions',
            'growth_estimates',
        )}
```

*(Mesmo padrão aplicado em `Fundamentals`, `Holders`, `FundsData`, `Domain`, `Quote`, `FastInfo`, `Calendars`, `PriceHistory`.)*

---

## 3.3 too-many-branches (R0912)

### 3.3.1 `yfinance/screener/screener.py` → `screen()`

**Problema:** Árvore condicional profunda distinguindo query predefinida vs custom, equity vs fund vs ETF, limites de paginação.

**Solução:** Early return por tipo de query + helpers dedicados por ramo (`_fetch_predefined_screen` vs `_post_custom_screen`), eliminando aninhamento.

**Antes (trecho):**
```python
if isinstance(query, str):
    if offset is not None:
        ...
    elif query in PREDEFINED_SCREENER_QUERIES:
        ...
    else:
        ...
else:
    if isinstance(query, EquityQuery):
        ...
    elif isinstance(query, FundQuery):
        ...
```

**Depois (trecho):**
```python
if isinstance(query, str):
    return _fetch_predefined_screen(...)
post_query = _build_custom_post_query(query, fields, defaults)
_assign_quote_type(post_query, query)
return _post_custom_screen(data, post_query, params_dict)
```

---

### 3.3.2 `yfinance/base.py` → `get_shares_full()`

**Problema:** Método com múltiplos ramos para parsing de datas, fetch HTTP, tratamento de erro JSON e construção da série.

**Solução:** Guard clauses + extração em `_parse_shares_date_range`, `_fetch_shares_json`, `_parse_shares_series`. Método público com fluxo sequencial e retornos antecipados.

**Antes (trecho):**
```python
def get_shares_full(self, start=None, end=None):
    # parsing de datas, validação, URL, try/except HTTP,
    # parsing JSON, error codes, DataFrame — tudo aninhado (~20 branches)
    ...
```

**Depois (trecho):**
```python
def get_shares_full(self, start=None, end=None):
    start, end = self._parse_shares_date_range(start, end, logger)
    if start is None:
        return None
    json_data = self._fetch_shares_json(start, end, logger)
    if json_data is None:
        return None
    df = self._parse_shares_series(json_data, logger)
    ...
    return df.sort_index()
```

---

## Resultado quantitativo

| Smell | ID | Antes | Depois |
|:---|:---:|:---:|:---:|
| too-many-statements | R0915 | 12 | **0** |
| too-many-instance-attributes | R0902 | 11 | **0** |
| too-many-branches | R0912 | 18 | **0** |

Comando de verificação:
```bash
python -m pylint yfinance --disable=all --enable=R0915,R0902,R0912
```
