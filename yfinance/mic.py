# This file maps Market Identifier Codes (MIC) to Yahoo Finance market suffixes.
# c.f. :
# https://help.yahoo.com/kb/finance-for-web/SLN2310.html;_ylt=AwrJKiCZFo9g3Y8AsDWPAwx.;_ylu=Y29sbwMEcG9zAzEEdnRpZAMEc2VjA3Ny?locale=en_US
# https://www.iso20022.org/market-identifier-codes

_MIC_TO_YAHOO_SUFFIX = {
    # United States
    'XCBT': '.CBT', 'XCME': '.CME', 'IFUS': '.NYB', 'CECS': '.CMX', 
    'XNYM': '.NYM', 'XNYS': '', 'XNAS': '',
    # Argentina
    'XBUE': '.BA',
    # Austria
    'XVIE': '.VI',
    # Australia
    'XASX': '.AX', 'XAUS': '.XA',
    # Belgium
    'XBRU': '.BR',
    # Brazil
    'BVMF': '.SA',
    # Canada
    'CNSX': '.CN', 'NEOE': '.NE', 'XTSE': '.TO', 'XTSX': '.V',
    # Chile
    'XSGO': '.SN',
    # China
    'XSHG': '.SS', 'XSHE': '.SZ',
    # Colombia
    'XBOG': '.CL',
    # Czech Republic
    'XPRA': '.PR',
    # Denmark
    'XCSE': '.CO',
    # Egypt
    'XCAI': '.CA',
    # Estonia
    'XTAL': '.TL',
    # Europe (Cboe Europe, Euronext)
    'CEUX': '.XD', 'XEUR': '.NX',
    # Finland
    'XHEL': '.HE',
    # France
    'XPAR': '.PA',
    # Germany
    'XBER': '.BE', 'XBMS': '.BM', 'XDUS': '.DU', 'XFRA': '.F',
    'XHAM': '.HM', 'XHAN': '.HA', 'XMUN': '.MU', 'XSTU': '.SG',
    'XETR': '.DE',
    # Greece
    'XATH': '.AT',
    # Hong Kong
    'XHKG': '.HK',
    # Hungary
    'XBUD': '.BD',
    # Iceland
    'XICE': '.IC',
    # India
    'XBOM': '.BO', 'XNSE': '.NS',
    # Indonesia
    'XIDX': '.JK',
    # Ireland
    'XDUB': '.IR',
    # Israel
    'XTAE': '.TA',
    # Italy
    'MTAA': '.MI', 'EUTL': '.TI',
    # Japan
    'XTKS': '.T',
    # Kuwait
    'XKFE': '.KW',
    # Latvia
    'XRIS': '.RG',
    # Lithuania
    'XVIL': '.VS',
    # Malaysia
    'XKLS': '.KL',
    # Mexico
    'XMEX': '.MX',
    # Netherlands
    'XAMS': '.AS',
    # New Zealand
    'XNZE': '.NZ',
    # Norway
    'XOSL': '.OL',
    # Philippines
    'XPHS': '.PS',
    # Poland
    'XWAR': '.WA',
    # Portugal
    'XLIS': '.LS',
    # Qatar
    'XQAT': '.QA',
    # Romania
    'XBSE': '.RO',
    # Singapore
    'XSES': '.SI',
    # South Africa
    'XJSE': '.JO',
    # South Korea
    'XKRX': '.KS', 'KQKS': '.KQ',
    # Spain
    'BMEX': '.MC',
    # Saudi Arabia
    'XTAD': '.SAU',
    # Sweden
    'XSTO': '.ST',
    # Switzerland
    'XSWX': '.SW',
    # Taiwan
    'ROCO': '.TWO', 'XTAI': '.TW',
    # Thailand
    'XBKK': '.BK',
    # Turkey
    'XIST': '.IS',
    # UAE
    'XDFM': '.AE',
    # United Kingdom
    'AQXE': '.AQ', 'XCHI': '.XC', 'XLON': '.L', 'ILSE': '.IL',
    # Venezuela
    'XCAR': '.CR',
    # Vietnam
    'XSTC': '.VN'
}

def market_suffix(mic: str) -> str:
    """
    Return the Yahoo Finance market suffix corresponding to a given MIC (Market Identifier Code).

    Args:
        mic (str): 
            The Market Identifier Code (MIC) for the exchange or market, as defined by ISO 10383.
            Examples: "XPAR" (Euronext Paris), "XNYM" (NYMEX), "XCBT" (CBOT), "XNYS" (NYSE).

    Returns:
        str: 
            The Yahoo Finance market suffix, including the leading dot (e.g. ".PA", ".NYM"), 
            or an empty string if the MIC maps to no suffix (e.g. NYSE, NASDAQ).

    Raises:
        ValueError: If the MIC code is not supported.

    Notes:
        Yahoo Finance symbols are typically formed as:
            <BaseSymbol> + <MarketSuffix>

    Examples:
        >>> market_suffix("XPAR")
        '.PA'
        >>> market_suffix("XNYM")
        '.NYM'
        >>> market_suffix("XNYS")  # NYSE has no suffix
        ''
    """
    mic_upper = mic.upper()
    if mic_upper not in _MIC_TO_YAHOO_SUFFIX:
        raise ValueError(f"Unknown MIC code: {mic_upper}")
    return _MIC_TO_YAHOO_SUFFIX[mic_upper]    


def yahoo_ticker(symbol: str, mic: str) -> str:
    """
    Build a full Yahoo Finance ticker symbol from a base symbol and MIC code.

    Args:
        symbol (str):
            The base instrument symbol, e.g. "AAPL", "OR", "PETR4".
        mic (str):
            The Market Identifier Code (MIC) for the exchange where the instrument is listed,
            e.g. "XPAR" (Euronext Paris), "XNYS" (NYSE), "BVMF" (Bovespa).

    Returns:
        str:
            The Yahoo Finance ticker, constructed by concatenating the base symbol
            and the Yahoo market suffix mapped from the MIC code.

    Raises:
        ValueError: If the MIC code is not found in the mapping list.

    Examples:
        >>> yahoo_ticker("OR", "XPAR")
        'OR.PA'
        >>> yahoo_ticker("AAPL", "XNYS")
        'AAPL'
        >>> yahoo_ticker("PETR4", "BVMF")
        'PETR4.SA'
        >>> yahoo_ticker("XYZ", "XXXX")
        Traceback (most recent call last):
            ...
        ValueError: Unknown MIC code: XXXX
    """
    return f"{symbol}{market_suffix(mic)}"
