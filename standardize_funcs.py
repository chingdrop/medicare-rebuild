import re
import html
import pandas as pd

from enums import alert_team_list, insurance_keywords, nurse_list, state_abbreviations


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


def standardize_call_time(call_time) -> int:
    if not call_time:
        return 0
    return int(pd.to_timedelta(str(call_time)).total_seconds())


def standardize_note_types(note_type: str):
    if not note_type:
        return None
    if note_type == 'Initial Evaluation with APRN':
        note_type = 'Initial Evaluation'
    return note_type.split(',')[0]


def standardize_patients(patient_df: pd.DataFrame) -> pd.DataFrame:
    patient_df['First Name'] = patient_df['First Name'].apply(standardize_name, args=(r'[^a-zA-Z\s.-]',))
    patient_df['Last Name'] = patient_df['Last Name'].apply(standardize_name, args=(r'[^a-zA-Z\s.-]',))
    patient_df['Full Name'] = patient_df['First Name'] + ' ' + patient_df['Last Name']
    patient_df['Middle Name'] = patient_df['Middle Name'].apply(standardize_name, args=(r'[^a-zA-Z-\s]',))
    patient_df['Nickname'] = patient_df['Nickname'].str.strip().str.title()
    patient_df['Phone Number'] = patient_df['Phone Number'].astype(str).str.replace(r'\D', '', regex=True)
    patient_df['Gender'] = patient_df['Gender'].replace({'Male': 'M', 'Female': 'F'})
    patient_df['Email'] = patient_df['Email'].apply(standardize_email)
    patient_df['Suffix'] = patient_df['Suffix'].str.strip().str.title()
    patient_df['Social Security'] = patient_df['Social Security'].astype(str).str.replace(r'\D', '', regex=True)

    # The logic in standardize name can be used for address text as well.
    patient_df['Mailing Address'] = patient_df['Mailing Address'].apply(standardize_name, args=(r'[^a-zA-Z0-9\s#.-/]',))
    patient_df['City'] = patient_df['City'].apply(standardize_name, args=(r'[^a-zA-Z-]',))
    patient_df['State'] = patient_df['State'].apply(standardize_state)
    patient_df['Zip code'] = patient_df['Zip code'].astype(str).str.split('-', n=1).str[0]

    patient_df['Medicare ID number'] = patient_df['Medicare ID number'].apply(standardize_mbi)
    patient_df['DX_Code'] = patient_df['DX_Code'].apply(standardize_dx_code)
    patient_df['Insurance ID:'] = patient_df['Insurance ID:'].apply(standardize_insurance_id)
    patient_df['InsuranceID2'] = patient_df['InsuranceID2'].apply(standardize_insurance_id)
    patient_df['Insurance Name:'] = patient_df.apply(fill_primary_payer, axis=1)
    patient_df['Insurance ID:'] = patient_df.apply(fill_primary_payer_id, axis=1)
    patient_df['Insurance Name:'] = patient_df['Insurance Name:'].apply(standardize_insurance_name)
    patient_df['InsuranceName2'] = patient_df['InsuranceName2'].apply(standardize_insurance_name)

    previous_patient_statuses = {
        'DO NOT CALL': 'Do Not Call' ,
        'In-Active': 'Inactive',
        'On-Board': 'Onboard'
    }
    patient_df['Member_Status'] = patient_df['Member_Status'].replace(previous_patient_statuses)
    patient_df = patient_df.rename(
        columns={
            'First Name': 'first_name',
            'Last Name': 'last_name',
            'Middle Name': 'middle_name',
            'Suffix': 'name_suffix',
            'Full Name': 'full_name',
            'Nickname': 'nick_name',
            'DOB': 'date_of_birth',
            'Gender': 'sex',
            'Email': 'email',
            'Phone Number': 'phone_number',
            'Social Security': 'social_security',
            'ID': 'sharepoint_id',
            'Mailing Address': 'street_address',
            'City': 'city',
            'State': 'temp_state',
            'Zip code': 'zipcode',
            'Medicare ID number': 'medicare_beneficiary_id',
            'Insurance ID:': 'primary_payer_id',
            'Insurance Name:': 'primary_payer_name',
            'InsuranceID2': 'secondary_payer_id',
            'InsuranceName2': 'secondary_payer_name',
            'On-board Date': 'evaluation_datetime',
            'DX_Code': 'temp_dx_code',
            'Member_Status': 'temp_status_type'
        }
    )
    return patient_df


def standardize_patient_notes(patient_note_df: pd.DataFrame) -> pd.DataFrame:
    patient_note_df['Recording_Time'] = patient_note_df['Recording_Time'].apply(standardize_call_time)
    patient_note_df.loc[patient_note_df['LCH_UPN'].isin(nurse_list), 'Recording_Time'] = 900

    patient_note_df['Notes'] = patient_note_df['Notes'].apply(html.unescape)
    patient_note_df['Notes'] = patient_note_df['Notes'].str.replace(r'<.*?>', '', regex=True)

    patient_note_df['Time_Note'] = patient_note_df['Time_Note'].apply(standardize_note_types)
    patient_note_df.loc[patient_note_df['LCH_UPN'].isin(nurse_list), 'Time_Note'] = 'Initial Evaluation'
    patient_note_df.loc[patient_note_df['LCH_UPN'].isin(alert_team_list), 'Time_Note'] = 'Alert'

    patient_note_df['SharePoint_ID'] = pd.to_numeric(patient_note_df['SharePoint_ID'], errors='coerce', downcast='integer')
    # Boolean column is flipped because it's stored differently in the database.
    patient_note_df['Auto_Time'] = patient_note_df['Auto_Time'].replace({True: 0, False: 1})
    patient_note_df['Auto_Time'] = patient_note_df['Auto_Time'].astype('Int64')
    patient_note_df = patient_note_df.rename(
        columns={
            'SharePoint_ID': 'sharepoint_id',
            'Notes': 'note_content',
            'TimeStamp': 'note_datetime',
            'LCH_UPN': 'temp_user',
            'Time_Note': 'temp_note_type',
            'Recording_Time': 'call_time_seconds',
            'Auto_Time': 'is_manual',
            'Start_Time': 'start_call_datetime',
            'End_Time': 'end_call_datetime'
        }
    )
    return patient_note_df


def check_database_constraints(df: pd.DataFrame) -> pd.DataFrame:
    failed_df = df[df['phone_number'].apply(lambda x: len(str(x)) != 10)]
    success_df = df[df['phone_number'].apply(lambda x: len(str(x)) == 10)]
    failed_df = df[df['social_security'].apply(lambda x: len(str(x)) != 9)]
    success_df = df[df['social_security'].apply(lambda x: len(str(x)) == 9)]
    failed_df = df[df['zipcode'].apply(lambda x: len(str(x)) != 5)]
    success_df = df[df['zipcode'].apply(lambda x: len(str(x)) == 5)]
    failed_df = df[df['medicare_beneficiary_id'].apply(lambda x: len(str(x)) != 11)]
    success_df = df[df['medicare_beneficiary_id'].apply(lambda x: len(str(x)) == 11)]
    # failed_df = export_df[export_df[['First Name', 'Last Name', 'ID', 'DOB', 'Phone Number', 'Gender', 'Mailing Address', 'City', 'State', 'Zip code', 'DX_Code']].isnull().any(axis=1)]
    # failed_df = export_df[export_df[['Insurance ID:', 'Insurance Name:', 'Medicare ID number']].isnull().all(axis=1)]
    return success_df, failed_df