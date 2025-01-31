import re

import pandas as pd

from enums import insurance_keywords, state_abbreviations


def standardize_name(name: str, pattern: str) -> str:
    """Standardizes strings from name-like texts.
    Trims whitespace and titles the text. Flattens remaining whitespace to one space.
    Uses inverse regex pattern to replace all unmatched characters with empty string.

    Args:
        name (string): The value to be standardized.
        pattern (string): The inverse pattern used in replacing unwanted characters. 

    Returns:
        string: The standardized name text.    
    """
    name = str(name).strip().title()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(pattern, '', name)
    return name

def standardize_state(state):
    state = str(state).strip().title()
    state = state_abbreviations.get(state, state).upper()
    if state == 'NAN':
        return None
    return state

def standardize_dx_code(dx_code):
    dx_code = str(dx_code).strip().upper()
    matches = re.finditer(r'[E|I|R]\d+(\.\d+)?', dx_code)
    matches = [match.group(0).replace('.', '') for match in matches]
    return ','.join(matches)

    name = str(name).strip().title()
    for standard_name, keyword_sets in insurance_keywords.items():
        for keyword_set in keyword_sets:
            if all(re.search(r'\b' + re.escape(keyword) + r'\b', name.lower()) for keyword in keyword_set):
                return standard_name
    return name

    insurance_id = str(ins_id).strip().upper()
    insurance_id = re.sub(r'[^A-Z0-9]', '', insurance_id)
    id_pattern = r'([A-Z]*\d+[A-Z]*\d+[A-Z]*\d+[A-Z]*\d*)'
    id_in_id = re.search(id_pattern, insurance_id)
    if id_in_id:
        return id_in_id.group(0)
    return insurance_id

def fill_primary_payer(row):
    if pd.isnull(row['Insurance Name:']) and pd.isnull(row['Insurance ID:']) and not pd.isnull(row['Medicare ID number']):
        return 'Medicare Part B'
    return row['Insurance Name:']

def fill_primary_payer_id(row):
    if row['Insurance Name:'] == 'Medicare Part B' and pd.isnull(row['Insurance ID:']) :
        return row['Medicare ID number']
    return row['Insurance ID:']