# FM product group (sheet Страхование) → dashboard dimensions
FM_PRODUCT_GROUPS: dict[str, dict[str, str]] = {
    "property": {
        "label": "Имущественное страхование",
        "insurance_type": "Voluntary",
        "insurance_type_ru": "Добровольное",
        "category": "Property Insurance",
        "category_ru": "Имущественное страхование",
        "product_name": "Имущественное страхование",
    },
    "financial_risks": {
        "label": "Финансовые риски",
        "insurance_type": "Voluntary",
        "insurance_type_ru": "Добровольное",
        "category": "Financial Risks",
        "category_ru": "Финансовые риски",
        "product_name": "Финансовые риски",
    },
    "personal": {
        "label": "Личное страхование",
        "insurance_type": "Voluntary",
        "insurance_type_ru": "Добровольное",
        "category": "Personal Insurance",
        "category_ru": "Личное страхование",
        "product_name": "Личное страхование",
    },
    "liability": {
        "label": "Страхование ответственности",
        "insurance_type": "Voluntary",
        "insurance_type_ru": "Добровольное",
        "category": "Liability Insurance",
        "category_ru": "Страхование ответственности",
        "product_name": "Страхование ответственности",
    },
    "compulsory": {
        "label": "Обязательное страхование",
        "insurance_type": "Compulsory",
        "insurance_type_ru": "Обязательное",
        "category": "Compulsory Insurance",
        "category_ru": "Обязательное страхование",
        "product_name": "Обязательное страхование",
    },
    "auto": {
        "label": "Авто страхование",
        "insurance_type": "Voluntary",
        "insurance_type_ru": "Добровольное",
        "category": "Auto Insurance",
        "category_ru": "Авто страхование",
        "product_name": "Авто страхование",
    },
}

# Sheet Страхование — direct premium by FM product group (company consolidated block)
INSURANCE_SHEET = "Страхование"
INSURANCE_SECTION = "direct_premium_by_product"
INSURANCE_PRODUCT_ROWS: dict[int, str] = {
    166: "property",
    170: "financial_risks",
    176: "personal",
    181: "liability",
    187: "compulsory",
    190: "auto",
}

# Sheet ФО — insurance reserve balances (balance sheet block)
FO_RESERVE_SECTION = "insurance_reserves"
FO_RESERVE_METRIC_ROWS: dict[int, str] = {
    311: "unearned_premium_reserve",
    312: "ibnr_reserve",
    313: "rbns_reserve",
    314: "stabilization_reserve_base",
    315: "stabilization_reserve_additional",
    316: "total_insurance_reserves",
    360: "total_assets",
}

# Phase 3 — sheet ФО (extended operating / financial income rows, company block)
FO_EXTENDED_METRIC_ROWS: dict[int, str] = {
    78: "insurance_service_revenue",
    96: "selling_expenses",
    97: "admin_expenses",
    98: "other_operating_expenses",
    99: "other_operating_income",
    104: "dividend_income",
    105: "interest_income",
    106: "fx_gain_loss",
    107: "other_financial_income",
}

# Phase 3 — sheet Регулятор (company consolidated solvency block)
REGULATOR_SHEET = "Регулятор"
REGULATOR_SECTION = "solvency_consolidated"
REGULATOR_METRIC_ROWS: dict[int, str] = {
    80: "own_funds_sources",
    101: "required_solvency_ratio_threshold",
    103: "required_solvency_margin",
    108: "actual_solvency_margin",
    109: "solvency_adequacy_ratio",
}

# Phase 3 — sheet Инвестиция (portfolio and financial income)
INVESTMENT_SHEET = "Инвестиция"
INVESTMENT_SECTION = "portfolio_consolidated"
INVESTMENT_METRIC_ROWS: dict[int, str] = {
    65: "dividend_income",
    66: "interest_income",
    67: "fx_gain_loss",
    68: "other_financial_income",
    109: "deposits_portfolio",
    110: "securities_portfolio",
    111: "total_investment_portfolio",
}

# Sheet Коэффиценты — official plan ratio coefficients (decimals in Excel, e.g. 0.3675)
# Row definitions (client-confirmed):
#   82 loss_ratio                       — Коэффициент убыточности (USE for loss-ratio visual)
#   83 commission_ratio                 — Коэффициент комиссионных
#   84 cost_ratio                       — Коэффициент себестоимости
#   85 mgmt_commercial_expense_ratio    — Коэффициент управленческих и коммерческих расходов
#   86 total_expense_ratio              — Коэффициент расходов (= 82+83+84+85)
# Do NOT use 82+83+84 as "loss ratio". Do NOT label row 85 as workbook "Коэффициент расходов".
COEFFICIENTS_SHEET = "Коэффиценты"
COEFFICIENTS_SECTION = "plan_ratios"
COEFFICIENTS_METRIC_ROWS: dict[int, str] = {
    82: "loss_ratio",
    83: "commission_ratio",
    84: "cost_ratio",
    85: "mgmt_commercial_expense_ratio",
    86: "total_expense_ratio",
}

# Phase 4 — initiative sheets (long-format, product × line item × month)
COMMERCIAL_INITIATIVES_SHEET = "Коммерческие инициативы"
COMMERCIAL_INITIATIVES_SECTION = "commercial_initiatives"
REINSURANCE_INITIATIVES_SHEET = "Инициативы по перестрахованию"
REINSURANCE_INITIATIVES_SECTION = "reinsurance_initiatives"
BACKOFFICE_INITIATIVES_SHEET = "Инициативы бек-офиса"
BACKOFFICE_INITIATIVES_SECTION = "backoffice_initiatives"

DIGITAL_CHANNEL_INITIATIVE = "Развитие прямых цифровых каналов"

# FM initiative name → dashboard priority_area
INITIATIVE_PRIORITY_AREA: dict[str, str] = {
    "Лидерство в Авто страховании": "Motor",
    "Лидерство в Банкинге": "Banking",
    "Развитие партнерства с Анорбанк": "Banking",
    "Развитие Партнерств": "Banking",
}

# FM grouping → dashboard priority_area (fallback when initiative is not mapped)
GROUPING_PRIORITY_AREA: dict[str, str] = {
    "Страхование авто": "Motor",
    "Имущественое страхование": "Property",
    "Обязательные виды страхования": "Motor",
    "Страхование ответственности": "Other",
    "Личное страхование": "Other",
    "Страхование финансовых рисков": "Other",
}

# FM grouping → insurance_type
GROUPING_INSURANCE_TYPE: dict[str, str] = {
    "Обязательные виды страхование": "Compulsory",
    "Обязательные виды страхования": "Compulsory",
}

# Initiative sheet layout (0-based column / row indices)
COMMERCIAL_INITIATIVE_LAYOUT = {
    "initiative_col": 7,
    "product_col": 8,
    "grouping_col": 9,
    "entity_type_col": 11,
    "line_item_col": 12,
    "date_row": 2,
    "data_start_row": 5,
    "value_start_col": 13,
}

REINSURANCE_INITIATIVE_LAYOUT = {
    "product_col": 2,
    "line_item_col": 3,
    "date_row": 4,
    "data_start_row": 6,
    "value_start_col": 4,
}

BACKOFFICE_INITIATIVE_LAYOUT = {
    "initiative_col": 1,
    "line_item_col": 2,
    "date_row": 4,
    "data_start_row": 7,
    "value_start_col": 3,
}
