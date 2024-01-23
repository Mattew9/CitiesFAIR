class Constants:
    """FREQUENCIES: Class of constant used to decipher the accrual periodicity of the datasets"""
    STR_LUN = 120
    FREQUENCIES = {
        'ANNUAL': 365 * 24,
        'ANNUAL_2': 180 * 24,
        'ANNUAL_3': 120 * 24,
        'BIDECENNIAL': 20 * 365 * 24,
        'BIENNIAL': 2 * 365 * 24,
        'BIHOURLY': 2,
        'BIMONTHLY': 60 * 24,
        'BIWEEKLY': 14 * 24,
        'CONT': 24,
        'DAILY': 24,
        'DAILY_2': 12,
        'DECENNIAL': 10 * 365 * 24,
        'HOURLY': 1,
        'IRREG': None,
        'MONTHLY': 28 * 24,
        'MONTHLY_2': 14 * 24,
        'MONTHLY_3': 10 * 24,
        'NEVER': None,
        'OTHER': None,
        'QUADRENNIAL': 4 * 365 * 24,
        'QUINQUENNIAL': 5 * 365 * 24,
        'TRIDECENNIAL': 30 * 365 * 24,
        'TRIENNIAL': 3 * 365 * 24,
        'TRIHOURLY': 3,
        'UNKNOWN': None,
        'UPDATE_CONT': 1,
        'WEEKLY': 7 * 24,
        'WEEKLY_2': 3 * 24,
        'WEEKLY_3': 2 * 24
    }
