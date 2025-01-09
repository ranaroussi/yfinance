_ROOT_URL_ = "finance.yahoo.com"
_QUERY1_URL_ = f"query1.{_ROOT_URL_}"
_BASE_URL_ = f"query2.{_ROOT_URL_}"


fundamentals_keys = {
    'financials': ["TaxEffectOfUnusualItems", "TaxRateForCalcs", "NormalizedEBITDA", "NormalizedDilutedEPS",
                   "NormalizedBasicEPS", "TotalUnusualItems", "TotalUnusualItemsExcludingGoodwill",
                   "NetIncomeFromContinuingOperationNetMinorityInterest", "ReconciledDepreciation",
                   "ReconciledCostOfRevenue", "EBITDA", "EBIT", "NetInterestIncome", "InterestExpense",
                   "InterestIncome", "ContinuingAndDiscontinuedDilutedEPS", "ContinuingAndDiscontinuedBasicEPS",
                   "NormalizedIncome", "NetIncomeFromContinuingAndDiscontinuedOperation", "TotalExpenses",
                   "RentExpenseSupplemental", "ReportedNormalizedDilutedEPS", "ReportedNormalizedBasicEPS",
                   "TotalOperatingIncomeAsReported", "DividendPerShare", "DilutedAverageShares", "BasicAverageShares",
                   "DilutedEPS", "DilutedEPSOtherGainsLosses", "TaxLossCarryforwardDilutedEPS",
                   "DilutedAccountingChange", "DilutedExtraordinary", "DilutedDiscontinuousOperations",
                   "DilutedContinuousOperations", "BasicEPS", "BasicEPSOtherGainsLosses", "TaxLossCarryforwardBasicEPS",
                   "BasicAccountingChange", "BasicExtraordinary", "BasicDiscontinuousOperations",
                   "BasicContinuousOperations", "DilutedNIAvailtoComStockholders", "AverageDilutionEarnings",
                   "NetIncomeCommonStockholders", "OtherunderPreferredStockDividend", "PreferredStockDividends",
                   "NetIncome", "MinorityInterests", "NetIncomeIncludingNoncontrollingInterests",
                   "NetIncomeFromTaxLossCarryforward", "NetIncomeExtraordinary", "NetIncomeDiscontinuousOperations",
                   "NetIncomeContinuousOperations", "EarningsFromEquityInterestNetOfTax", "TaxProvision",
                   "PretaxIncome", "OtherIncomeExpense", "OtherNonOperatingIncomeExpenses", "SpecialIncomeCharges",
                   "GainOnSaleOfPPE", "GainOnSaleOfBusiness", "OtherSpecialCharges", "WriteOff",
                   "ImpairmentOfCapitalAssets", "RestructuringAndMergernAcquisition", "SecuritiesAmortization",
                   "EarningsFromEquityInterest", "GainOnSaleOfSecurity", "NetNonOperatingInterestIncomeExpense",
                   "TotalOtherFinanceCost", "InterestExpenseNonOperating", "InterestIncomeNonOperating",
                   "OperatingIncome", "OperatingExpense", "OtherOperatingExpenses", "OtherTaxes",
                   "ProvisionForDoubtfulAccounts", "DepreciationAmortizationDepletionIncomeStatement",
                   "DepletionIncomeStatement", "DepreciationAndAmortizationInIncomeStatement", "Amortization",
                   "AmortizationOfIntangiblesIncomeStatement", "DepreciationIncomeStatement", "ResearchAndDevelopment",
                   "SellingGeneralAndAdministration", "SellingAndMarketingExpense", "GeneralAndAdministrativeExpense",
                   "OtherGandA", "InsuranceAndClaims", "RentAndLandingFees", "SalariesAndWages", "GrossProfit",
                   "CostOfRevenue", "TotalRevenue", "ExciseTaxes", "OperatingRevenue", "LossAdjustmentExpense",
                   "NetPolicyholderBenefitsAndClaims", "PolicyholderBenefitsGross", "PolicyholderBenefitsCeded",
                   "OccupancyAndEquipment", "ProfessionalExpenseAndContractServicesExpense", "OtherNonInterestExpense"],
    'balance-sheet': ["TreasurySharesNumber", "PreferredSharesNumber", "OrdinarySharesNumber", "ShareIssued", "NetDebt",
                      "TotalDebt", "TangibleBookValue", "InvestedCapital", "WorkingCapital", "NetTangibleAssets",
                      "CapitalLeaseObligations", "CommonStockEquity", "PreferredStockEquity", "TotalCapitalization",
                      "TotalEquityGrossMinorityInterest", "MinorityInterest", "StockholdersEquity",
                      "OtherEquityInterest", "GainsLossesNotAffectingRetainedEarnings", "OtherEquityAdjustments",
                      "FixedAssetsRevaluationReserve", "ForeignCurrencyTranslationAdjustments",
                      "MinimumPensionLiabilities", "UnrealizedGainLoss", "TreasuryStock", "RetainedEarnings",
                      "AdditionalPaidInCapital", "CapitalStock", "OtherCapitalStock", "CommonStock", "PreferredStock",
                      "TotalPartnershipCapital", "GeneralPartnershipCapital", "LimitedPartnershipCapital",
                      "TotalLiabilitiesNetMinorityInterest", "TotalNonCurrentLiabilitiesNetMinorityInterest",
                      "OtherNonCurrentLiabilities", "LiabilitiesHeldforSaleNonCurrent", "RestrictedCommonStock",
                      "PreferredSecuritiesOutsideStockEquity", "DerivativeProductLiabilities", "EmployeeBenefits",
                      "NonCurrentPensionAndOtherPostretirementBenefitPlans", "NonCurrentAccruedExpenses",
                      "DuetoRelatedPartiesNonCurrent", "TradeandOtherPayablesNonCurrent",
                      "NonCurrentDeferredLiabilities", "NonCurrentDeferredRevenue",
                      "NonCurrentDeferredTaxesLiabilities", "LongTermDebtAndCapitalLeaseObligation",
                      "LongTermCapitalLeaseObligation", "LongTermDebt", "LongTermProvisions", "CurrentLiabilities",
                      "OtherCurrentLiabilities", "CurrentDeferredLiabilities", "CurrentDeferredRevenue",
                      "CurrentDeferredTaxesLiabilities", "CurrentDebtAndCapitalLeaseObligation",
                      "CurrentCapitalLeaseObligation", "CurrentDebt", "OtherCurrentBorrowings", "LineOfCredit",
                      "CommercialPaper", "CurrentNotesPayable", "PensionandOtherPostRetirementBenefitPlansCurrent",
                      "CurrentProvisions", "PayablesAndAccruedExpenses", "CurrentAccruedExpenses", "InterestPayable",
                      "Payables", "OtherPayable", "DuetoRelatedPartiesCurrent", "DividendsPayable", "TotalTaxPayable",
                      "IncomeTaxPayable", "AccountsPayable", "TotalAssets", "TotalNonCurrentAssets",
                      "OtherNonCurrentAssets", "DefinedPensionBenefit", "NonCurrentPrepaidAssets",
                      "NonCurrentDeferredAssets", "NonCurrentDeferredTaxesAssets", "DuefromRelatedPartiesNonCurrent",
                      "NonCurrentNoteReceivables", "NonCurrentAccountsReceivable", "FinancialAssets",
                      "InvestmentsAndAdvances", "OtherInvestments", "InvestmentinFinancialAssets",
                      "HeldToMaturitySecurities", "AvailableForSaleSecurities",
                      "FinancialAssetsDesignatedasFairValueThroughProfitorLossTotal", "TradingSecurities",
                      "LongTermEquityInvestment", "InvestmentsinJointVenturesatCost",
                      "InvestmentsInOtherVenturesUnderEquityMethod", "InvestmentsinAssociatesatCost",
                      "InvestmentsinSubsidiariesatCost", "InvestmentProperties", "GoodwillAndOtherIntangibleAssets",
                      "OtherIntangibleAssets", "Goodwill", "NetPPE", "AccumulatedDepreciation", "GrossPPE", "Leases",
                      "ConstructionInProgress", "OtherProperties", "MachineryFurnitureEquipment",
                      "BuildingsAndImprovements", "LandAndImprovements", "Properties", "CurrentAssets",
                      "OtherCurrentAssets", "HedgingAssetsCurrent", "AssetsHeldForSaleCurrent", "CurrentDeferredAssets",
                      "CurrentDeferredTaxesAssets", "RestrictedCash", "PrepaidAssets", "Inventory",
                      "InventoriesAdjustmentsAllowances", "OtherInventories", "FinishedGoods", "WorkInProcess",
                      "RawMaterials", "Receivables", "ReceivablesAdjustmentsAllowances", "OtherReceivables",
                      "DuefromRelatedPartiesCurrent", "TaxesReceivable", "AccruedInterestReceivable", "NotesReceivable",
                      "LoansReceivable", "AccountsReceivable", "AllowanceForDoubtfulAccountsReceivable",
                      "GrossAccountsReceivable", "CashCashEquivalentsAndShortTermInvestments",
                      "OtherShortTermInvestments", "CashAndCashEquivalents", "CashEquivalents", "CashFinancial",
                      "CashCashEquivalentsAndFederalFundsSold"],
    'cash-flow': ["ForeignSales", "DomesticSales", "AdjustedGeographySegmentData", "FreeCashFlow",
                  "RepurchaseOfCapitalStock", "RepaymentOfDebt", "IssuanceOfDebt", "IssuanceOfCapitalStock",
                  "CapitalExpenditure", "InterestPaidSupplementalData", "IncomeTaxPaidSupplementalData",
                  "EndCashPosition", "OtherCashAdjustmentOutsideChangeinCash", "BeginningCashPosition",
                  "EffectOfExchangeRateChanges", "ChangesInCash", "OtherCashAdjustmentInsideChangeinCash",
                  "CashFlowFromDiscontinuedOperation", "FinancingCashFlow", "CashFromDiscontinuedFinancingActivities",
                  "CashFlowFromContinuingFinancingActivities", "NetOtherFinancingCharges", "InterestPaidCFF",
                  "ProceedsFromStockOptionExercised", "CashDividendsPaid", "PreferredStockDividendPaid",
                  "CommonStockDividendPaid", "NetPreferredStockIssuance", "PreferredStockPayments",
                  "PreferredStockIssuance", "NetCommonStockIssuance", "CommonStockPayments", "CommonStockIssuance",
                  "NetIssuancePaymentsOfDebt", "NetShortTermDebtIssuance", "ShortTermDebtPayments",
                  "ShortTermDebtIssuance", "NetLongTermDebtIssuance", "LongTermDebtPayments", "LongTermDebtIssuance",
                  "InvestingCashFlow", "CashFromDiscontinuedInvestingActivities",
                  "CashFlowFromContinuingInvestingActivities", "NetOtherInvestingChanges", "InterestReceivedCFI",
                  "DividendsReceivedCFI", "NetInvestmentPurchaseAndSale", "SaleOfInvestment", "PurchaseOfInvestment",
                  "NetInvestmentPropertiesPurchaseAndSale", "SaleOfInvestmentProperties",
                  "PurchaseOfInvestmentProperties", "NetBusinessPurchaseAndSale", "SaleOfBusiness",
                  "PurchaseOfBusiness", "NetIntangiblesPurchaseAndSale", "SaleOfIntangibles", "PurchaseOfIntangibles",
                  "NetPPEPurchaseAndSale", "SaleOfPPE", "PurchaseOfPPE", "CapitalExpenditureReported",
                  "OperatingCashFlow", "CashFromDiscontinuedOperatingActivities",
                  "CashFlowFromContinuingOperatingActivities", "TaxesRefundPaid", "InterestReceivedCFO",
                  "InterestPaidCFO", "DividendReceivedCFO", "DividendPaidCFO", "ChangeInWorkingCapital",
                  "ChangeInOtherWorkingCapital", "ChangeInOtherCurrentLiabilities", "ChangeInOtherCurrentAssets",
                  "ChangeInPayablesAndAccruedExpense", "ChangeInAccruedExpense", "ChangeInInterestPayable",
                  "ChangeInPayable", "ChangeInDividendPayable", "ChangeInAccountPayable", "ChangeInTaxPayable",
                  "ChangeInIncomeTaxPayable", "ChangeInPrepaidAssets", "ChangeInInventory", "ChangeInReceivables",
                  "ChangesInAccountReceivables", "OtherNonCashItems", "ExcessTaxBenefitFromStockBasedCompensation",
                  "StockBasedCompensation", "UnrealizedGainLossOnInvestmentSecurities", "ProvisionandWriteOffofAssets",
                  "AssetImpairmentCharge", "AmortizationOfSecurities", "DeferredTax", "DeferredIncomeTax",
                  "DepreciationAmortizationDepletion", "Depletion", "DepreciationAndAmortization",
                  "AmortizationCashFlow", "AmortizationOfIntangibles", "Depreciation", "OperatingGainsLosses",
                  "PensionAndEmployeeBenefitExpense", "EarningsLossesFromEquityInvestments",
                  "GainLossOnInvestmentSecurities", "NetForeignCurrencyExchangeGainLoss", "GainLossOnSaleOfPPE",
                  "GainLossOnSaleOfBusiness", "NetIncomeFromContinuingOperations",
                  "CashFlowsfromusedinOperatingActivitiesDirect", "TaxesRefundPaidDirect", "InterestReceivedDirect",
                  "InterestPaidDirect", "DividendsReceivedDirect", "DividendsPaidDirect", "ClassesofCashPayments",
                  "OtherCashPaymentsfromOperatingActivities", "PaymentsonBehalfofEmployees",
                  "PaymentstoSuppliersforGoodsandServices", "ClassesofCashReceiptsfromOperatingActivities",
                  "OtherCashReceiptsfromOperatingActivities", "ReceiptsfromGovernmentGrants", "ReceiptsfromCustomers"]}

_PRICE_COLNAMES_ = ['Open', 'High', 'Low', 'Close', 'Adj Close']

quote_summary_valid_modules = (
    "summaryProfile",  # contains general information about the company
    "summaryDetail",  # prices + volume + market cap + etc
    "assetProfile",  # summaryProfile + company officers
    "fundProfile",
    "price",  # current prices
    "quoteType",  # quoteType
    "esgScores",  # Environmental, social, and governance (ESG) scores, sustainability and ethical performance of companies
    "incomeStatementHistory",
    "incomeStatementHistoryQuarterly",
    "balanceSheetHistory",
    "balanceSheetHistoryQuarterly",
    "cashFlowStatementHistory",
    "cashFlowStatementHistoryQuarterly",
    "defaultKeyStatistics",  # KPIs (PE, enterprise value, EPS, EBITA, and more)
    "financialData",  # Financial KPIs (revenue, gross margins, operating cash flow, free cash flow, and more)
    "calendarEvents",  # future earnings date
    "secFilings",  # SEC filings, such as 10K and 10Q reports
    "upgradeDowngradeHistory",  # upgrades and downgrades that analysts have given a company's stock
    "institutionOwnership",  # institutional ownership, holders and shares outstanding
    "fundOwnership",  # mutual fund ownership, holders and shares outstanding
    "majorDirectHolders",
    "majorHoldersBreakdown",
    "insiderTransactions",  # insider transactions, such as the number of shares bought and sold by company executives
    "insiderHolders",  # insider holders, such as the number of shares held by company executives
    "netSharePurchaseActivity",  # net share purchase activity, such as the number of shares bought and sold by company executives
    "earnings",  # earnings history
    "earningsHistory",
    "earningsTrend",  # earnings trend
    "industryTrend",
    "indexTrend",
    "sectorTrend",
    "recommendationTrend",
    "futuresChain",
)

# map last updated as of 2024.09.18
SECTOR_INDUSTY_MAPPING = {
    'basic-materials': {'specialty-chemicals',
                        'gold',
                        'building-materials',
                        'copper',
                        'steel',
                        'agricultural-inputs',
                        'chemicals',
                        'other-industrial-metals-mining',
                        'lumber-wood-production',
                        'aluminum',
                        'other-precious-metals-mining',
                        'coking-coal',
                        'paper-paper-products',
                        'silver'},
    'communication-services': {'internet-content-information',
                                'telecom-services',
                                'entertainment',
                                'electronic-gaming-multimedia',
                                'advertising-agencies',
                                'broadcasting',
                                'publishing'},
    'consumer-cyclical': {'internet-retail',
                            'auto-manufacturers',
                            'restaurants',
                            'home-improvement-retail',
                            'travel-services',
                            'specialty-retail',
                            'apparel-retail',
                            'residential-construction',
                            'footwear-accessories',
                            'packaging-containers',
                            'lodging',
                            'auto-parts',
                            'auto-truck-dealerships',
                            'gambling',
                            'resorts-casinos',
                            'leisure',
                            'apparel-manufacturing',
                            'personal-services',
                            'furnishings-fixtures-appliances',
                            'recreational-vehicles',
                            'luxury-goods',
                            'department-stores',
                            'textile-manufacturing'},
    'consumer-defensive': {'discount-stores',
                            'beverages-non-alcoholic',
                            'household-personal-products',
                            'packaged-foods',
                            'tobacco',
                            'confectioners',
                            'farm-products',
                            'food-distribution',
                            'grocery-stores',
                            'beverages-brewers',
                            'education-training-services',
                            'beverages-wineries-distilleries'},
    'energy': {'oil-gas-integrated',
                'oil-gas-midstream',
                'oil-gas-e-p',
                'oil-gas-equipment-services',
                'oil-gas-refining-marketing',
                'uranium',
                'oil-gas-drilling',
                'thermal-coal'},
    'financial-services': {'banks-diversified',
                            'credit-services',
                            'asset-management',
                            'insurance-diversified',
                            'banks-regional',
                            'capital-markets',
                            'financial-data-stock-exchanges',
                            'insurance-property-casualty',
                            'insurance-brokers',
                            'insurance-life',
                            'insurance-specialty',
                            'mortgage-finance',
                            'insurance-reinsurance',
                            'shell-companies',
                            'financial-conglomerates'},
    'healthcare': {'drug-manufacturers-general',
                    'healthcare-plans',
                    'biotechnology',
                    'medical-devices',
                    'diagnostics-research',
                    'medical-instruments-supplies',
                    'medical-care-facilities',
                    'drug-manufacturers-specialty-generic',
                    'health-information-services',
                    'medical-distribution',
                    'pharmaceutical-retailers'},
    'industrials': {'aerospace-defense',
                    'specialty-industrial-machinery',
                    'railroads',
                    'building-products-equipment',
                    'farm-heavy-construction-machinery',
                    'specialty-business-services',
                    'integrated-freight-logistics',
                    'waste-management',
                    'conglomerates',
                    'industrial-distribution',
                    'engineering-construction',
                    'rental-leasing-services',
                    'consulting-services',
                    'trucking',
                    'electrical-equipment-parts',
                    'airlines',
                    'tools-accessories',
                    'pollution-treatment-controls',
                    'security-protection-services',
                    'marine-shipping',
                    'metal-fabrication',
                    'infrastructure-operations',
                    'staffing-employment-services',
                    'airports-air-services',
                    'business-equipment-supplies'},
    'real-estate': {'reit-specialty',
                    'reit-industrial',
                    'reit-retail',
                    'reit-residential',
                    'reit-healthcare-facilities',
                    'real-estate-services',
                    'reit-office',
                    'reit-diversified',
                    'reit-mortgage',
                    'reit-hotel-motel',
                    'real-estate-development',
                    'real-estate-diversified'},
    'technology': {'software-infrastructure',
                    'semiconductors',
                    'consumer-electronics',
                    'software-application',
                    'information-technology-services',
                    'semiconductor-equipment-materials',
                    'communication-equipment',
                    'computer-hardware',
                    'electronic-components',
                    'scientific-technical-instruments',
                    'solar',
                    'electronics-computer-distribution'},
    'utilities': {'utilities-regulated-electric',
                    'utilities-renewable',
                    'utilities-diversified',
                    'utilities-regulated-gas',
                    'utilities-independent-power-producers',
                    'utilities-regulated-water'}
}

def merge_two_level_dicts(dict1, dict2):
    result = dict1.copy()
    for key, value in dict2.items():
        if key in result:
            # If both are sets, merge them
            if isinstance(value, set) and isinstance(result[key], set):
                result[key] = result[key] | value
            # If both are dicts, merge their contents
            elif isinstance(value, dict) and isinstance(result[key], dict):
                result[key] = {
                    k: (result[key].get(k, set()) | v if isinstance(v, set) 
                        else v) if k in result[key]
                    else v
                    for k, v in value.items()
                }
        else:
            result[key] = value
    return result

EQUITY_SCREENER_EQ_MAP = {
    "exchange": {
        'ar': {'BUE'},
        'at': {'VIE'},
        'au': {'ASX'},
        'be': {'BRU'},
        'br': {'SAO'},
        'ca': {'CNQ', 'NEO', 'TOR', 'VAN'},
        'ch': {'EBS'},
        'cl': {'SGO'},
        'cn': {'SHH', 'SHZ'},
        'co': {'BVC'},
        'cz': {'PRA'},
        'de': {'BER', 'DUS', 'FRA', 'HAM', 'GER', 'MUN', 'STU'},
        'dk': {'CPH'},
        'ee': {'TAL'},
        'eg': {'CAI'},
        'es': {'MCE'},
        'fi': {'HEL'},
        'fr': {'PAR'},
        'gb': {'AQS', 'IOB', 'LSE'},
        'gr': {'ATH'},
        'hk': {'HKG'},
        'hu': {'BUD'},
        'id': {'JKT'},
        'ie': {'ISE'},
        'il': {'TLV'},
        'in': {'BSE', 'NSI'},
        'is': {'ICE'},
        'it': {'MIL'},
        'jp': {'FKA', 'JPX', 'SAP'},
        'kr': {'KOE', 'KSC'},
        'kw': {'KUW'},
        'lk': {},
        'lt': {'LIT'},
        'lv': {'RIS'},
        'mx': {'MEX'},
        'my': {'KLS'},
        'nl': {'AMS'},
        'no': {'OSL'},
        'nz': {'NZE'},
        'pe': {},
        'ph': {'PHP', 'PHS'},
        'pk': {},
        'pl': {'WSE'},
        'pt': {'LIS'},
        'qa': {'DOH'},
        'ro': {'BVB'},
        'ru': {},
        'sa': {'SAU'},
        'se': {'STO'},
        'sg': {'SES'},
        'sr': {},
        'th': {'SET'},
        'tr': {'IST'},
        'tw': {'TAI', 'TWO'},
        'us': {'ASE', 'BTS', 'CXI', 'NCM', 'NGM', 'NMS', 'NYQ', 'OEM', 'OQB', 'OQX', 'PCX', 'PNK', 'YHD'},
        've': {'CCS'},
        'vn': {},
        'za': {'JNB'}
    },
    "sector": {
        "Basic Materials", "Industrials", "Communication Services", "Healthcare",
        "Real Estate", "Technology", "Energy", "Utilities", "Financial Services",
        "Consumer Defensive", "Consumer Cyclical"
    },
    "peer_group": {
        "US Fund Equity Energy",
        "US CE Convertibles",
        "EAA CE UK Large-Cap Equity",
        "EAA CE Other",
        "US Fund Financial",
        "India CE Multi-Cap",
        "US Fund Foreign Large Blend",
        "US Fund Consumer Cyclical",
        "EAA Fund Global Equity Income",
        "China Fund Sector Equity Financial and Real Estate",
        "US Fund Equity Precious Metals",
        "EAA Fund RMB Bond - Onshore",
        "China Fund QDII Greater China Equity",
        "US Fund Large Growth",
        "EAA Fund Germany Equity",
        "EAA Fund Hong Kong Equity",
        "EAA CE UK Small-Cap Equity",
        "US Fund Natural Resources",
        "US CE Preferred Stock",
        "India Fund Sector - Financial Services",
        "US Fund Diversified Emerging Mkts",
        "EAA Fund South Africa & Namibia Equity",
        "China Fund QDII Sector Equity",
        "EAA CE Sector Equity Biotechnology",
        "EAA Fund Switzerland Equity",
        "US Fund Large Value",
        "EAA Fund Asia ex-Japan Equity",
        "US Fund Health",
        "US Fund China Region",
        "EAA Fund Emerging Europe ex-Russia Equity",
        "EAA Fund Sector Equity Industrial Materials",
        "EAA Fund Japan Large-Cap Equity",
        "EAA Fund EUR Corporate Bond",
        "US Fund Technology",
        "EAA CE Global Large-Cap Blend Equity",
        "Mexico Fund Mexico Equity",
        "US Fund Trading--Leveraged Equity",
        "EAA Fund Sector Equity Consumer Goods & Services",
        "US Fund Large Blend",
        "EAA Fund Global Flex-Cap Equity",
        "EAA Fund EUR Aggressive Allocation - Global",
        "EAA Fund China Equity",
        "EAA Fund Global Large-Cap Growth Equity",
        "US CE Options-based",
        "EAA Fund Sector Equity Financial Services",
        "EAA Fund Europe Large-Cap Blend Equity",
        "EAA Fund China Equity - A Shares",
        "EAA Fund USD Corporate Bond",
        "EAA Fund Eurozone Large-Cap Equity",
        "China Fund Aggressive Allocation Fund",
        "EAA Fund Sector Equity Technology",
        "EAA Fund Global Emerging Markets Equity",
        "EAA Fund EUR Moderate Allocation - Global",
        "EAA Fund Other Bond",
        "EAA Fund Denmark Equity",
        "EAA Fund US Large-Cap Blend Equity",
        "India Fund Large-Cap",
        "Paper & Forestry",
        "Containers & Packaging",
        "US Fund Miscellaneous Region",
        "Energy Services",
        "EAA Fund Other Equity",
        "Homebuilders",
        "Construction Materials",
        "China Fund Equity Funds",
        "Steel",
        "Consumer Durables",
        "EAA Fund Global Large-Cap Blend Equity",
        "Transportation Infrastructure",
        "Precious Metals",
        "Building Products",
        "Traders & Distributors",
        "Electrical Equipment",
        "Auto Components",
        "Construction & Engineering",
        "Aerospace & Defense",
        "Refiners & Pipelines",
        "Diversified Metals",
        "Textiles & Apparel",
        "Industrial Conglomerates",
        "Household Products",
        "Commercial Services",
        "Food Retailers",
        "Semiconductors",
        "Media",
        "Automobiles",
        "Consumer Services",
        "Technology Hardware",
        "Transportation",
        "Telecommunication Services",
        "Oil & Gas Producers",
        "Machinery",
        "Retailing",
        "Healthcare",
        "Chemicals",
        "Food Products",
        "Diversified Financials",
        "Real Estate",
        "Insurance",
        "Utilities",
        "Pharmaceuticals",
        "Software & Services",
        "Banks"
    }
}
EQUITY_SCREENER_EQ_MAP['region'] = EQUITY_SCREENER_EQ_MAP['exchange'].keys()
ordered_keys = ['region'] + [k for k in EQUITY_SCREENER_EQ_MAP.keys() if k != 'region']
EQUITY_SCREENER_EQ_MAP = {k:EQUITY_SCREENER_EQ_MAP[k] for k in ordered_keys}
FUND_SCREENER_EQ_MAP = {
    "exchange": {
        'us': {'NAS'}
    }
}
COMMON_SCREENER_FIELDS = {
    "price":{
        "eodprice",
        "intradaypricechange",
        "intradayprice"
    },
    "eq_fields": {
        "exchange"}, 
}
FUND_SCREENER_FIELDS = {
    "eq_fields": {
        "categoryname",
        "performanceratingoverall",
        "initialinvestment", 
        "annualreturnnavy1categoryrank", 
        "riskratingoverall"}
}
FUND_SCREENER_FIELDS = merge_two_level_dicts(FUND_SCREENER_FIELDS, COMMON_SCREENER_FIELDS)
EQUITY_SCREENER_FIELDS = {
    "eq_fields": {
        "region",
        "sector",
        "peer_group"}, 
    "price":{
        "lastclosemarketcap.lasttwelvemonths",
        "percentchange",
        "lastclose52weekhigh.lasttwelvemonths",
        "fiftytwowkpercentchange",
        "lastclose52weeklow.lasttwelvemonths",
        "intradaymarketcap"},
    "trading":{
        "beta",
        "avgdailyvol3m",
        "pctheldinsider",
        "pctheldinst",
        "dayvolume",
        "eodvolume"},
    "short_interest":{
        "short_percentage_of_shares_outstanding.value",
        "short_interest.value",
        "short_percentage_of_float.value",
        "days_to_cover_short.value",
        "short_interest_percentage_change.value"},
    "valuation":{
        "bookvalueshare.lasttwelvemonths",
        "lastclosemarketcaptotalrevenue.lasttwelvemonths",
        "lastclosetevtotalrevenue.lasttwelvemonths",
        "pricebookratio.quarterly",
        "peratio.lasttwelvemonths",
        "lastclosepricetangiblebookvalue.lasttwelvemonths",
        "lastclosepriceearnings.lasttwelvemonths",
        "pegratio_5y"},
    "profitability":{
        "consecutive_years_of_dividend_growth_count",
        "returnonassets.lasttwelvemonths",
        "returnonequity.lasttwelvemonths",
        "forward_dividend_per_share",
        "forward_dividend_yield",
        "returnontotalcapital.lasttwelvemonths"},
    "leverage":{
        "lastclosetevebit.lasttwelvemonths",
        "netdebtebitda.lasttwelvemonths",
        "totaldebtequity.lasttwelvemonths",
        "ltdebtequity.lasttwelvemonths",
        "ebitinterestexpense.lasttwelvemonths",
        "ebitdainterestexpense.lasttwelvemonths",
        "lastclosetevebitda.lasttwelvemonths",
        "totaldebtebitda.lasttwelvemonths"},
    "liquidity":{
        "quickratio.lasttwelvemonths",
        "altmanzscoreusingtheaveragestockinformationforaperiod.lasttwelvemonths",
        "currentratio.lasttwelvemonths",
        "operatingcashflowtocurrentliabilities.lasttwelvemonths"},
    "income_statement":{
        "totalrevenues.lasttwelvemonths",
        "netincomemargin.lasttwelvemonths",
        "grossprofit.lasttwelvemonths",
        "ebitda1yrgrowth.lasttwelvemonths",
        "dilutedepscontinuingoperations.lasttwelvemonths",
        "quarterlyrevenuegrowth.quarterly",
        "epsgrowth.lasttwelvemonths",
        "netincomeis.lasttwelvemonths",
        "ebitda.lasttwelvemonths",
        "dilutedeps1yrgrowth.lasttwelvemonths",
        "totalrevenues1yrgrowth.lasttwelvemonths",
        "operatingincome.lasttwelvemonths",
        "netincome1yrgrowth.lasttwelvemonths",
        "grossprofitmargin.lasttwelvemonths",
        "ebitdamargin.lasttwelvemonths",
        "ebit.lasttwelvemonths",
        "basicepscontinuingoperations.lasttwelvemonths",
        "netepsbasic.lasttwelvemonths"
        "netepsdiluted.lasttwelvemonths"},
    "balance_sheet":{
        "totalassets.lasttwelvemonths",
        "totalcommonsharesoutstanding.lasttwelvemonths",
        "totaldebt.lasttwelvemonths",
        "totalequity.lasttwelvemonths",
        "totalcurrentassets.lasttwelvemonths",
        "totalcashandshortterminvestments.lasttwelvemonths",
        "totalcommonequity.lasttwelvemonths",
        "totalcurrentliabilities.lasttwelvemonths",
        "totalsharesoutstanding"},
    "cash_flow":{
        "forward_dividend_yield",
        "leveredfreecashflow.lasttwelvemonths",
        "capitalexpenditure.lasttwelvemonths",
        "cashfromoperations.lasttwelvemonths",
        "leveredfreecashflow1yrgrowth.lasttwelvemonths",
        "unleveredfreecashflow.lasttwelvemonths",
        "cashfromoperations1yrgrowth.lasttwelvemonths"},
    "esg":{
        "esg_score",
        "environmental_score",
        "governance_score",
        "social_score",
        "highest_controversy"}
}
EQUITY_SCREENER_FIELDS = merge_two_level_dicts(EQUITY_SCREENER_FIELDS, COMMON_SCREENER_FIELDS)

USER_AGENTS = [
    # Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",

    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Mozilla/5.0 (X11; Linux i686; rv:135.0) Gecko/20100101 Firefox/135.0",

    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",

    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/131.0.2903.86"
]
