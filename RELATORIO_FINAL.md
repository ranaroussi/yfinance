# Relatório Final

Manutenção De Software - 01 - 2026.1
Profa. Carla Ilane Moreira Bezerra

Autores:
[PREENCHER PELO ALUNO: Nome 1]
[PREENCHER PELO ALUNO: Nome 2]

## 1. Caracterização do Projeto

**Nome Projeto:** yfinance (v1.4.1)

O **yfinance** é uma biblioteca Python open-source para download e consulta de dados financeiros e de mercado via APIs públicas do Yahoo! Finance. Segundo o README e o `setup.py`, o projeto se propõe a oferecer uma interface *Pythonic* para obter cotações, históricos de preços, informações fundamentalistas, notícias, setores/indústrias e funcionalidades de streaming. Entre os principais componentes estão `Ticker` e `Tickers` (dados de um ou vários ativos), `download` (download em lote), `Search` (busca de cotações e notícias), `Market`, `Sector`, `Industry`, `Screener` (filtros de mercado), `WebSocket`/`AsyncWebSocket` (dados ao vivo) e utilitários de calendário. O pacote é distribuído sob licença Apache 2.0, destina-se a pesquisa e uso educacional, e não é afiliado ao Yahoo, Inc.

## 2. Métricas Antes da Refatoração

### 2.1 Radon Antes da Refatoração

#### 2.1.1 Complexidade Ciclomática por Arquivo Antes da Refatoração

**Tabela de piores métricas**

| arquivo | funções | cc_media | cc_max | cc_soma | pior_rank | pior_funcao |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| yfinance/scrapers/history.py | 20 | 39.65 | 245 | 793 | F | _fix_bad_div_adjust |
| yfinance/multi.py | 6 | 10.33 | 36 | 62 | E | _download_impl |
| yfinance/utils.py | 56 | 4.66 | 28 | 261 | D | safe_merge_dfs |
| yfinance/screener/screener.py | 1 | 24.00 | 24 | 24 | D | screen |
| yfinance/scrapers/quote.py | 52 | 3.87 | 18 | 201 | C | _fetch_info |

**Tabela de melhores métricas**

| arquivo | funções | cc_media | cc_max | cc_soma | pior_rank | pior_funcao |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| yfinance/exceptions.py | 8 | 1.12 | 2 | 9 | A | __init__ |
| yfinance/config.py | 10 | 1.40 | 3 | 14 | A | __getattr__ |
| yfinance/_http.py | 5 | 2.20 | 3 | 11 | A | _warn_once_on_fallback |
| yfinance/__init__.py | 1 | 3.00 | 3 | 3 | A | set_config |
| yfinance/domain/domain.py | 14 | 1.21 | 3 | 17 | A | _parse_top_companies |

#### 2.1.2 Complexidade Ciclomática por Função Antes da Refatoração

**Tabela de piores métricas**

| arquivo | tipo | nome | rank_cc | complexity |
| :---: | :---: | :---: | :---: | :---: |
| yfinance/scrapers/history.py | method | _fix_bad_div_adjust | F | 245 |
| yfinance/scrapers/history.py | method | history | F | 153 |
| yfinance/scrapers/history.py | method | _fix_prices_sudden_change | F | 147 |
| yfinance/scrapers/history.py | method | _reconstruct_intervals_batch | F | 96 |
| yfinance/multi.py | function | _download_impl | E | 36 |

**Tabela de melhores métricas**

| arquivo | tipo | nome | rank_cc | complexity |
| :---: | :---: | :---: | :---: | :---: |
| yfinance/base.py | method | history | A | 1 |
| yfinance/base.py | method | get_recommendations_summary | A | 1 |
| yfinance/base.py | method | get_calendar | A | 1 |
| yfinance/base.py | method | get_sec_filings | A | 1 |
| yfinance/base.py | method | get_info | A | 1 |

#### 2.1.3 Índice de Manutenibilidade Antes da Refatoração

**Tabela de piores métricas**

| arquivo | mi | rank_mi |
| :---: | :---: | :---: |
| yfinance/scrapers/history.py | 0.00 | C |
| yfinance/utils.py | 3.97 | C |
| yfinance/scrapers/quote.py | 6.80 | C |
| yfinance/cache.py | 20.63 | A |
| yfinance/data.py | 27.09 | A |

**Tabela de melhores métricas**

| arquivo | mi | rank_mi |
| :---: | :---: | :---: |
| yfinance/shared.py | 100.00 | A |
| yfinance/version.py | 100.00 | A |
| yfinance/domain/__init__.py | 100.00 | A |
| yfinance/scrapers/__init__.py | 100.00 | A |
| yfinance/screener/__init__.py | 100.00 | A |

#### 2.1.4 Métricas brutas Antes da Refatoração

| loc total | lloc total | sloc total | comments total | multi |
| :---: | :---: | :---: | :---: | :---: |
| 12978 | 7892 | 9170 | 1185 | 976 |

### 2.2 CodeCarbon Antes da Refatoração

| duration | emissions | emissions_rate | cpu_energy | ram_energy | energy_consumed |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 383.92 | 0.000885 | 2.30×10⁻⁶ | 0.004742 | 0.002056 | 0.008997 |

*(Fonte: `metrics-before-codecarbon/emissions_antes.csv`, projeto `yfinance_antes`.)*

### 2.3 Pylint Antes da Refatoração

#### 2.3.1 Distribuição do Smells Antes da Refatoração

Principais smells detectados (`metrics-before-pylint/pylint_ranking_smells_antes.json`):

| Smell | Ocorrências |
| :---: | :---: |
| line-too-long | 442 |
| missing-function-docstring | 222 |
| trailing-whitespace | 98 |
| logging-fstring-interpolation | 95 |
| invalid-name | 66 |
| protected-access | 51 |
| too-many-branches | **18** |
| too-many-statements | **12** |
| too-many-instance-attributes | **11** |
| too-many-locals | 16 |

#### 2.3.2 Distribuição dos Problemas de Código Antes da Refatoração

| Categoria | Ocorrências |
| :---: | :---: |
| convention | 993 |
| warning | 249 |
| refactor | 167 |
| error | 16 |

#### 2.3.3 Arquivos mais Críticos Antes da Refatoração

| Arquivo | Ocorrências |
| :---: | :---: |
| yfinance/scrapers/history.py | 326 |
| yfinance/utils.py | 140 |
| yfinance/scrapers/quote.py | 131 |
| yfinance/const.py | 109 |
| yfinance/cache.py | 105 |
| yfinance/base.py | 86 |
| yfinance/data.py | 72 |
| yfinance/ticker.py | 59 |
| yfinance/screener/screener.py | 48 |
| yfinance/screener/query.py | 40 |

#### 2.3.4 Score Projeto Antes da Refatoração

**Score:** 7.86/10

### 2.4 Pytest Antes da Refatoração

**Cobertura Total:** 70,5%

**Testes que passaram:** 164

**Testes que falharam:** 17

*(Fontes: `htmlcov_antes/status.json` e `metrics-before-pytest/pytest_antes.html`.)*

---

## 3. Processo de Refatoração

### 3.1 too-many-statements

#### 3.1.1 Ocorrência Número 1

- **Arquivo/Função:** `yfinance/screener/screener.py` → `screen()`
- **Problema:** Função monolítica com mais de 50 *statements*, concentrando validação de parâmetros, resolução de query predefinida, montagem de payload HTTP e tratamento de erros no mesmo corpo (Pylint R0915).
- **Solução Aplicada:** Técnica **Extract Method** — extração de 8 funções privadas (`_validate_screen_limits`, `_resolve_predefined_with_offset`, `_fetch_predefined_screen`, `_build_custom_post_query`, etc.). A função pública tornou-se um orquestrador enxuto.

- **Trecho Antes:**

```python
def screen(query, offset=None, size=None, count=None, ...):
    # ... docstring ...
    if count is not None and count > 250:
        raise ValueError("Yahoo limits query count to 250, reduce count.")
    if size is not None and size > 250:
        raise ValueError("Yahoo limits query size to 250, reduce size.")
    if offset is not None and isinstance(query, str):
        post_query = PREDEFINED_SCREENER_QUERIES[query]
        query = post_query['query']
        # ... dezenas de statements inline ...
    if isinstance(query, str):
        params_dict['scrIds'] = query
        resp = _data.get(url=_PREDEFINED_URL_, params=params_dict)
        return resp.json()["finance"]["result"][0]
    elif isinstance(query, QueryBase):
        # ... montagem e POST customizado ...
    response = _data.post(_SCREENER_URL_, data=data, params=params_dict)
    return response.json()['finance']['result'][0]
```

- **Trecho Depois:**

```python
def screen(query, offset=None, size=None, count=None, ...):
    data = YfData(session=session)
    _validate_screen_limits(count, size)
    query, sortField, sortAsc, defaults = _resolve_predefined_with_offset(
        query, offset, sortField, sortAsc)
    params_dict = _build_screen_params()
    if isinstance(query, str):
        return _fetch_predefined_screen(data, query, fields, params_dict, size)
    if isinstance(query, QueryBase):
        post_query = _build_custom_post_query(query, fields, defaults)
        _assign_quote_type(post_query, query)
        return _post_custom_screen(data, post_query, params_dict)
    raise ValueError(f'Query must be type str or QueryBase, not "{type(query)}"')
```

### 3.2 too-many-instance-attributes

#### 3.2.1 Ocorrência Número 1

- **Arquivo/Função:** `yfinance/search.py` → classe `Search`
- **Problema:** Mais de 15 atributos de instância no `__init__` (parâmetros de busca, estado de resposta e logger), violando o limite de 7 atributos do Pylint (R0902).
- **Solução Aplicada:** **Introdução de objeto de estado interno** — agrupamento dos parâmetros em `self._params` (dict) e dos resultados em `self._results` (dict). Propriedades públicas (`quotes`, `news`, `all`, etc.) delegam a `_results`, preservando a API.

- **Trecho Antes:**

```python
class Search:
    def __init__(self, query, max_results=8, ...):
        self.session = session
        self._data = YfData(session=self.session)
        self.query = query
        self.max_results = max_results
        self.news_count = news_count
        self.lists_count = lists_count
        self.include_cb = include_cb
        # ... mais ~10 atributos self.* ...
        self._response = {}
        self._quotes = []
        self._news = []
        self._lists = []
        self._research = []
        self._nav = []
```

- **Trecho Depois:**

```python
class Search:
    def __init__(self, query, max_results=8, ...):
        self.session = session
        self._data = YfData(session=self.session)
        self._params = {
            'query': query, 'max_results': max_results,
            'news_count': news_count, 'lists_count': lists_count, ...
        }
        self.raise_errors = raise_errors
        self._logger = utils.get_yf_logger()
        self._results = {
            'response': {}, 'quotes': [], 'news': [],
            'lists': [], 'research': [], 'nav': [], 'all': {},
        }

    @property
    def quotes(self):
        return self._results['quotes']
```

### 3.3 too-many-branches

#### 3.3.1 Ocorrência Número 1

- **Arquivo/Função:** `yfinance/base.py` → `get_shares_full()`
- **Problema:** Método com árvore condicional profunda para parsing de datas, requisição HTTP, validação de JSON e construção de série temporal (Pylint R0912, 20+ ramos).
- **Solução Aplicada:** **Guard clauses** + **Extract Method** — extração em `_parse_shares_date_range`, `_fetch_shares_json` e `_parse_shares_series`. O método público ficou com fluxo linear e retornos antecipados.

- **Trecho Antes:**

```python
def get_shares_full(self, start=None, end=None):
    logger = utils.get_yf_logger()
    tz = self._get_ticker_tz(timeout=10)
    dt_now = pd.Timestamp.now('UTC').tz_convert(tz)
    if start is not None:
        start = utils._parse_user_dt(start, tz)
    if end is not None:
        end = utils._parse_user_dt(end, tz)
    # ... validação, URL, try/except, parsing JSON, error codes ...
    try:
        json_data = self._data.cache_get(url=shares_url).json()
    except (...):
        # ... múltiplos ramos de erro ...
    # ... construção do DataFrame inline ...
    return df.sort_index()
```

- **Trecho Depois:**

```python
def get_shares_full(self, start=None, end=None):
    logger = utils.get_yf_logger()
    start, end = self._parse_shares_date_range(start, end, logger)
    if start is None:
        return None
    json_data = self._fetch_shares_json(start, end, logger)
    if json_data is None:
        return None
    df = self._parse_shares_series(json_data, logger)
    if df is None:
        return None
    tz = self._get_ticker_tz(timeout=10)
    df.index = df.index.tz_localize(tz)
    return df.sort_index()
```

---

## 4. Métricas Após a Refatoração

### 4.1 Radon Após a Refatoração

#### 4.1.1 Complexidade Ciclomática por Arquivo Após a Refatoração

**Tabela de piores métricas**

| arquivo | funções | cc_media | cc_max | cc_soma | pior_rank | pior_funcao |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| yfinance/utils.py | 72 | 3.86 | 25 | 278 | D | camel2title |
| yfinance/multi.py | 13 | 5.54 | 18 | 72 | C | reindex_dfs |
| yfinance/scrapers/quote.py | 52 | 3.94 | 18 | 205 | C | _fetch_info |
| yfinance/data.py | 29 | 4.41 | 16 | 128 | C | _accept_consent_form |
| yfinance/scrapers/history.py | 151 | 5.54 | 15 | 836 | C | _div_adjust_cluster_too_big |

**Tabela de melhores métricas**

| arquivo | funções | cc_media | cc_max | cc_soma | pior_rank | pior_funcao |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| yfinance/exceptions.py | 8 | 1.12 | 2 | 9 | A | __init__ |
| yfinance/config.py | 10 | 1.40 | 3 | 14 | A | __getattr__ |
| yfinance/_http.py | 5 | 2.20 | 3 | 11 | A | _warn_once_on_fallback |
| yfinance/__init__.py | 1 | 3.00 | 3 | 3 | A | set_config |
| yfinance/domain/domain.py | 15 | 1.20 | 3 | 18 | A | _parse_top_companies |

#### 4.1.2 Complexidade Ciclomática por Função Após a Refatoração

**Tabela de piores métricas**

| arquivo | tipo | nome | rank_cc | complexity |
| :---: | :---: | :---: | :---: | :---: |
| yfinance/utils.py | function | camel2title | D | 25 |
| yfinance/multi.py | function | reindex_dfs | C | 18 |
| yfinance/scrapers/quote.py | method | _fetch_info | C | 18 |
| yfinance/data.py | method | _accept_consent_form | C | 16 |
| yfinance/scrapers/history.py | method | _div_adjust_cluster_too_big | C | 15 |

**Tabela de melhores métricas**

| arquivo | tipo | nome | rank_cc | complexity |
| :---: | :---: | :---: | :---: | :---: |
| yfinance/base.py | method | _quote | A | 1 |
| yfinance/base.py | method | history | A | 1 |
| yfinance/base.py | method | get_recommendations_summary | A | 1 |
| yfinance/base.py | method | get_calendar | A | 1 |
| yfinance/base.py | method | get_sec_filings | A | 1 |

#### 4.1.3 Índice de Manutenibilidade Após a Refatoração

**Tabela de piores métricas**

| arquivo | mi | rank_mi |
| :---: | :---: | :---: |
| yfinance/scrapers/history.py | 0.00 | C |
| yfinance/utils.py | 1.51 | C |
| yfinance/scrapers/quote.py | 6.89 | C |
| yfinance/cache.py | 20.63 | A |
| yfinance/base.py | 26.27 | A |

**Tabela de melhores métricas**

| arquivo | mi | rank_mi |
| :---: | :---: | :---: |
| yfinance/shared.py | 100.00 | A |
| yfinance/version.py | 100.00 | A |
| yfinance/domain/__init__.py | 100.00 | A |
| yfinance/scrapers/__init__.py | 100.00 | A |
| yfinance/screener/__init__.py | 100.00 | A |

#### 4.1.4 Métricas brutas Após a Refatoração

| loc total | lloc total | sloc total | comments total | multi |
| :---: | :---: | :---: | :---: | :---: |
| 13634 | 8554 | 9988 | 970 | 976 |

### 4.2 CodeCarbon Após a Refatoração

| duration | emissions | emissions_rate | cpu_energy | ram_energy | energy_consumed |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 405.81 | 0.000409 | 1.01×10⁻⁶ | 0.000711 | 0.002179 | 0.004163 |

*(Fonte: `metrics-after-codecarbon/emissions_depois.csv`, projeto `yfinance_depois`.)*

### 4.3 Pylint Após a Refatoração

#### 4.3.1 Distribuição do Smells Após a Refatoração

| Smell | Ocorrências |
| :---: | :---: |
| line-too-long | 478 |
| missing-function-docstring | 223 |
| unused-variable | 127 |
| possibly-unused-variable | 123 |
| invalid-name | 89 |
| trailing-whitespace | 84 |
| unused-argument | 76 |
| logging-fstring-interpolation | 66 |
| too-many-locals | 64 |
| bad-indentation | 51 |

**Nota:** `too-many-statements`, `too-many-instance-attributes` e `too-many-branches` **não aparecem** no ranking pós-refatoração (contagem = 0 em `pylint_refactor_depois.json`).

#### 4.3.2 Distribuição dos Problemas de Código Após a Refatoração

| Categoria | Ocorrências |
| :---: | :---: |
| convention | 1034 |
| warning | 569 |
| refactor | 231 |
| error | 22 |

#### 4.3.3 Arquivos mais Críticos Após a Refatoração

| Arquivo | Ocorrências |
| :---: | :---: |
| yfinance/scrapers/history.py | 796 |
| yfinance/scrapers/quote.py | 129 |
| yfinance/utils.py | 118 |
| yfinance/const.py | 109 |
| yfinance/cache.py | 105 |
| yfinance/base.py | 80 |
| yfinance/data.py | 74 |
| yfinance/ticker.py | 59 |
| yfinance/screener/screener.py | 41 |
| yfinance/screener/query.py | 40 |

#### 4.3.4 Score Projeto Após a Refatoração

**Score do Pylint:** 7.44/10

### 4.4 Pytest Após a Refatoração

#### 4.4.1 Cobertura de testes Após a Refatoração

**Cobertura Total:** 78%

#### 4.4.2 Testes Passaram vs Testes Falharam Após a Refatoração

**Quantidade de testes que passaram:** 175

**Quantidade de testes que falharam:** 6

*(Fontes: `htmlcov_depois/status.json` e `metrics-after-pytest/pytest_depois.html`.)*

---

## 5. Comparação dos Resultados

A refatoração atingiu integralmente a meta definida para os três code smells-alvo do Pylint. Antes da intervenção, `pylint_refactor_antes.json` registrava 41 ocorrências combinadas (R0915: 12, R0902: 11, R0912: 18); após a refatoração, `pylint_refactor_depois.json` aponta **zero** em todas as três categorias. Isso confirma que as técnicas de Extract Method, agrupamento de estado em estruturas internas e guard clauses eliminaram os smells exigidos sem alterar a API pública do pacote. O score global do Pylint caiu marginalmente (7,86 → 7,44), fenômeno esperado pelo acréscimo de helpers e linhas de código, além de smells de convenção não abordados neste trabalho (ex.: `line-too-long`, `missing-function-docstring`).

Quanto ao Radon, houve melhora expressiva na complexidade ciclomática dos pontos críticos. A pior função passou de CC = 245 (`_fix_bad_div_adjust` em `history.py`) para CC = 25 (`camel2title` em `utils.py`), redução de ~90%. O CC máximo de `history.py` caiu de 245 para 15, e o de `multi.py` de 36 para 18. Esse ganho estrutural indica código mais modular e testável. Por outro lado, o volume total de código aumentou (SLOC: 9.170 → 9.988), e o MI de `utils.py` piorou (3,97 → 1,51), efeito colateral da decomposição em muitas funções auxiliares. O arquivo `history.py` permanece com MI = 0,00 por ser o maior módulo do pacote, embora suas funções individuais tenham CC drasticamente menor.

No eixo ambiental e de testes, o CodeCarbon registrou queda de ~54% nas emissões (0,000885 → 0,000409 kg CO₂eq) e no consumo total de energia (0,008997 → 0,004163 kWh), apesar de duração ligeiramente maior (+5,7%). A cobertura de testes subiu de 70,5% para 78%, e o número de testes aprovados aumentou de 164 para 175, com falhas reduzidas de 17 para 6. As falhas remanescentes concentram-se em dependências externas (`lxml`), chamadas de rede e um teste de repair pré-existente, não indicando regressão funcional direta da refatoração dos smells-alvo.

---

## 6. Percepções sobre a Prática

O uso de uma LLM como assistente na refatoração do **yfinance** mostrou-se útil quando combinado com métricas objetivas e testes automatizados. A ferramenta acelerou tarefas repetitivas — identificar smells no Pylint, sugerir *Extract Method*, agrupar atributos em dicionários internos e aplicar *guard clauses* — e manteve coerência ao longo de dezenas de arquivos. Em módulos menores (`screener.py`, `search.py`, `base.py`), o ciclo *refatorar → validar com Pylint focalizado → rodar Pytest* funcionou de forma fluida, o que reduziu o tempo em relação a uma refatoração manual arquivo por arquivo.

Entre as facilidades, destacam-se: (1) a capacidade de decompor funções monolíticas preservando assinaturas públicas; (2) a padronização de soluções recorrentes — por exemplo, substituir múltiplos `self._cache_* = None` por um único `self._cache` — aplicada de forma uniforme em scrapers como `Analysis`, `Fundamentals` e `Quote`; (3) a geração de documentação e relatórios a partir dos artefatos já coletados (`metrics-before-*` e `metrics-after-*`), facilitando a comparação antes/depois exigida pelo trabalho.

As dificuldades, porém, reforçam que a LLM não substitui revisão humana nem validação contínua. Em `history.py`, a refatoração automatizada exigiu várias iterações e correções pontuais (métodos truncados, `continue` vs `return`, helpers sem `@staticmethod`). Substituições em massa no código — como trocar `self._` por `self._cache.` — quebraram referências legítimas (`self._data`, `self._symbol`) e nomes de métodos em `quote.py`. Tentativas iniciais com *dataclass* para agrupar atributos também dispararam R0902, levando à adoção de dicionários simples. Houve ainda regressão temporária em `FastInfo`, que acessava `self._tkr._quote` removido na refatoração de `TickerBase`, corrigida com uma *property* de compatibilidade.

Outro aprendizado foi metodológico: refatorar **um arquivo por vez**, com Pylint restrito aos smells-alvo e Pytest do módulo correspondente, evitou acumular erros difíceis de rastrear. A queda marginal do score global do Pylint (7,86 → 7,44), apesar de zerar os smells exigidos, mostra que a LLM tende a resolver o problema imediato, mas não necessariamente melhora convenções de estilo (`line-too-long`, docstrings ausentes em helpers novos). Por fim, a experiência confirma que LLMs são ferramentas produtivas em manutenção evolutiva de software open-source, desde que o engenheiro mantenha o controle da estratégia, valide cada etapa com ferramentas de QA e trate o código gerado como rascunho técnico sujeito a revisão — não como entrega final automática.

## 7. Links

Link do Repositório Fork: [PREENCHER PELO ALUNO]

Link dos Pull Requests abertos: [PREENCHER PELO ALUNO]
