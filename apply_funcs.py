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

def standardize_email(email: str) -> str:
    """Standardizes email address strings.
    Trims whitespace and lowers the text. Regex matching attempts to find email addresses and extracts them.

    Args:
        email (string): The value to be standardized.

    Returns:
        string: The standardized email address.    
    """
    email = str(email).strip().lower()
    email_pattern = r'(^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$)'
    email_match = re.search(email_pattern, email)
    if email_match:
        return email_match.group(0)
    return email

def standardize_state(state: str) -> str:
    """Standardizes US state strings.
    Trims whitespace and titles the text. 
    Searches state's name and correlates that with the State's two letter abbreviation.

    Args:
        state (string): The value to be standardized.

    Returns:
        string: The standardized US state abbreviation.    
    """
    state = str(state).strip().title()
    state = state_abbreviations.get(state, state).upper()
    if state == 'NAN':
        return None
    return state

def standardize_mbi(mbi: str) -> str:
    """Standardizes medicare beneficiary ID strings.
    Trims whitespace and lowers the text. 
    Regex matching attempts to find medicare beneficiary IDs and extracts them.

    Args:
        mbi (string): The value to be standardized.

    Returns:
        string: The standardized medicare beneficiary ID.    
    """
    mbi = str(mbi).strip().upper()
    mbi_pattern = r'([A-Z0-9]{11})'
    mbi_match = re.search(mbi_pattern, mbi)
    if mbi_match:
        return mbi_match.group(0)
    return mbi

def standardize_dx_code(dx_code: str) -> str:
    """Standardizes diagnosis codes.
    Trims whitespace and uppers the text. Searches text for regex pattern of diagnosis code. 
    Joins all elements into a single string with commas as the separator. 

    Args:
        dx_code (string): The value to be standardized.

    Returns:
        string: string representation of the list of Dx codes.
    """
    dx_code = str(dx_code).strip().upper()
    matches = re.finditer(r'[E|I|R]\d+(\.\d+)?', dx_code)
    matches = [match.group(0).replace('.', '') for match in matches]
    return ','.join(matches)

def standardize_insurance_name(name: str) -> str:
    """Standardizes insurance name strings.
    Trims whitespace and titles the text.
    Searches the insurance name for keywords and correlates that with a list of standard insurance names.
    Keywords are held in lists where all elements of the list must be present for a positive match.

    Args:
        name (string): The value to be standardized.

    Returns:
        string: The standardized insurance name.    
    """
    name = str(name).strip().title()
    for standard_name, keyword_sets in insurance_keywords.items():
        for keyword_set in keyword_sets:
            if all(re.search(r'\b' + re.escape(keyword) + r'\b', name.lower()) for keyword in keyword_set):
                return standard_name
    return name

def standardize_insurance_id(ins_id: str) -> str:
    """Standardizes insurance ID strings.
    Trims whitespace and uppers the text. Any non-alphanumeric character is replaced with empty string.
    Regex matching attempts to find insurance IDs and extracts them.

    Args:
        ins_id (string): The value to be standardized.

    Returns:
        string: The standardized insurance ID.    
    """
    insurance_id = str(ins_id).strip().upper()
    insurance_id = re.sub(r'[^A-Z0-9]', '', insurance_id)
    id_pattern = r'([A-Z]*\d+[A-Z]*\d+[A-Z]*\d+[A-Z]*\d*)'
    id_match = re.search(id_pattern, insurance_id)
    if id_match:
        return id_match.group(0)
    return insurance_id

def fill_primary_payer(row: pd.Series) -> pd.Series:
    """Fills primary payer name with 'Medicare Part B'.
    If insurance name and insurance ID is null; and medicare beneficiary ID is not null.
    Then the primary payer name gets filled with 'Medicare Part B'.

    Args:
        row (pandas.Series): Row of Dataframe to be standardized.

    Returns:
        pandas.Series: The standardized row of Dataframe.    
    """
    if pd.isnull(row['Insurance Name:']) and pd.isnull(row['Insurance ID:']) and not pd.isnull(row['Medicare ID number']):
        return 'Medicare Part B'
    return row['Insurance Name:']

def fill_primary_payer_id(row: pd.Series) -> pd.Series:
    """Fills primary payer ID with medicare beneficiary ID.
    If insurance name is 'Medicare Part B' and insurance ID is null.
    Then the primary payer ID gets filled with the medicare beneficiary ID.

    Args:
        row (pandas.Series): Row of Dataframe to be standardized.

    Returns:
        pandas.Series: The standardized row of Dataframe.    
    """
    if row['Insurance Name:'] == 'Medicare Part B' and pd.isnull(row['Insurance ID:']) :
        return row['Medicare ID number']
    return row['Insurance ID:']