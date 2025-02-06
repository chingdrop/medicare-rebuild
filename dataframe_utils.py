import re
import html
import pandas as pd

from enums import alert_team_list, insurance_keywords, nurse_list, state_abbreviations


def standardize_name(name: str, pattern: str) -> str:
    """Standardizes strings from name-like texts.
    Trims whitespace and titles the text. Flattens remaining whitespace to one space.
    Uses inverse regex pattern to replace all unmatched characters with empty string.

    Args:
        - name (string): The value to be standardized.
        - pattern (string): The inverse pattern used in replacing unwanted characters. 

    Returns:
        - string: The standardized name text.    
    """
    name = str(name).strip().title()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(pattern, '', name)
    return name


def standardize_email(email: str) -> str:
    """Standardizes email address strings.
    Trims whitespace and lowers the text. Regex matching attempts to find email addresses and extracts them.

    Args:
        - email (string): The value to be standardized.

    Returns:
        - string: The standardized email address.    
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
        - state (string): The value to be standardized.

    Returns:
        - string: The standardized US state abbreviation.    
    """
    state = str(state).strip().title()
    state = state_abbreviations.get(state, state).upper()
    # NAN is checked here because of the default values dict.get() returns.
    return state


def standardize_mbi(mbi: str) -> str:
    """Standardizes medicare beneficiary ID strings.
    Trims whitespace and lowers the text. 
    Regex matching attempts to find medicare beneficiary IDs and extracts them.

    Args:
        - mbi (string): The value to be standardized.

    Returns:
        - string: The standardized medicare beneficiary ID.    
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
        - dx_code (string): The value to be standardized.

    Returns:
        - string: string representation of the list of Dx codes.
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
        - name (string): The value to be standardized.

    Returns:
        - string: The standardized insurance name.    
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
        - ins_id (string): The value to be standardized.

    Returns:
        - string: The standardized insurance ID.    
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
        - row (pandas.Series): Row of Dataframe to be standardized.

    Returns:
        - pandas.Series: The standardized row of Dataframe.    
    """
    if pd.isnull(row['Insurance Name:']) and pd.isnull(row['Insurance ID:']) and not pd.isnull(row['Medicare ID number']):
        return 'Medicare Part B'
    return row['Insurance Name:']


def fill_primary_payer_id(row: pd.Series) -> pd.Series:
    """Fills primary payer ID with medicare beneficiary ID.
    If insurance name is 'Medicare Part B' and insurance ID is null.
    Then the primary payer ID gets filled with the medicare beneficiary ID.

    Args:
        - row (pandas.Series): Row of Dataframe to be standardized.

    Returns:
        - pandas.Series: The standardized row of Dataframe.    
    """
    if row['Insurance Name:'] == 'Medicare Part B' and pd.isnull(row['Insurance ID:']) :
        return row['Medicare ID number']
    return row['Insurance ID:']


def standardize_call_time(call_time) -> int:
    """Standardizes call time in seconds.
    Converts value to a timedelta object and then calculates total seconds.

    Args:
        - call_time: The value to be standardized.

    Returns:
        - int: The standardized call time in seconds.    
    """
    if not call_time:
        return 0
    return pd.to_timedelta(str(call_time)).total_seconds()


def standardize_note_types(note_type: str) -> str:
    """Standardizes note type value.
    Replaces common phrase for proper note type. Split note type by commas, then use the first element.

    Args:
        - note_type (string): The value to be standardized.

    Returns:
        - string: The standardized note type phrase.    
    """
    if note_type == 'Initial Evaluation with APRN':
        note_type = 'Initial Evaluation'
    return str(note_type).split(',')[0]


def standardize_vendor(row: pd.Series) -> pd.Series:
    """Original values had Vendor name in the Device name.
    If Vendor name is a substring of Device name then return the Vendor name.
    Else, if 'Tenvoi' or 'Omron' is in Device name then return 'Tenvoi' or 'Omron' respectively.

    Args:
        - row (pandas.Series): Row of Dataframe to be standardized.

    Returns:
        - pandas.Series: The standardized row of Dataframe.    
    """
    if not row['Vendor'] in row['Device_Name']:
        if 'Tenovi' in row['Device_Name']:
            return 'Tenovi'
        else:
            return 'Omron'
    return row['Vendor']


def standardize_patients(df: pd.DataFrame) -> pd.DataFrame:
    df['First Name'] = df['First Name'].apply(standardize_name, args=(r'[^a-zA-Z\s.-]',))
    df['Last Name'] = df['Last Name'].apply(standardize_name, args=(r'[^a-zA-Z\s.-]',))
    df['Full Name'] = df['First Name'] + ' ' + df['Last Name']
    df['Middle Name'] = df['Middle Name'].apply(standardize_name, args=(r'[^a-zA-Z-\s]',))
    df['Nickname'] = df['Nickname'].str.strip().str.title()
    df['Phone Number'] = df['Phone Number'].astype(str).str.replace(r'\D', '', regex=True)
    df['Gender'] = df['Gender'].replace({'Male': 'M', 'Female': 'F'})
    df['Email'] = df['Email'].apply(standardize_email)
    df['Suffix'] = df['Suffix'].str.strip().str.title()
    df['Social Security'] = df['Social Security'].astype(str).str.replace(r'\D', '', regex=True)

    # The logic in standardize name can be used for address text as well.
    df['Mailing Address'] = df['Mailing Address'].apply(standardize_name, args=(r'[^a-zA-Z0-9\s#.-/]',))
    df['City'] = df['City'].apply(standardize_name, args=(r'[^a-zA-Z-]',))
    df['State'] = df['State'].apply(standardize_state)
    df['Zip code'] = df['Zip code'].astype(str).str.split('-', n=1).str[0]

    df['Medicare ID number'] = df['Medicare ID number'].apply(standardize_mbi)
    df['DX_Code'] = df['DX_Code'].apply(standardize_dx_code)
    df['Insurance ID:'] = df['Insurance ID:'].apply(standardize_insurance_id)
    df['InsuranceID2'] = df['InsuranceID2'].apply(standardize_insurance_id)
    df['Insurance Name:'] = df.apply(fill_primary_payer, axis=1)
    df['Insurance ID:'] = df.apply(fill_primary_payer_id, axis=1)
    df['Insurance Name:'] = df['Insurance Name:'].apply(standardize_insurance_name)
    df['InsuranceName2'] = df['InsuranceName2'].apply(standardize_insurance_name)

    previous_patient_statuses = {
        'DO NOT CALL': 'Do Not Call' ,
        'In-Active': 'Inactive',
        'On-Board': 'Onboard'
    }
    df['Member_Status'] = df['Member_Status'].replace(previous_patient_statuses)
    df = df.rename(
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
    # Convert string Nan back to Null value.
    df.replace(r'(?i)^nan$', None, regex=True, inplace=True)
    return df


def standardize_patient_notes(df: pd.DataFrame) -> pd.DataFrame:
    df['Recording_Time'] = df['Recording_Time'].apply(standardize_call_time)
    df.loc[df['LCH_UPN'].isin(['Joycelynn Harris']), 'Recording_Time'] = 900

    df['Notes'] = df['Notes'].apply(html.unescape)
    df['Notes'] = df['Notes'].str.replace(r'<.*?>', '', regex=True)

    df['Time_Note'] = df['Time_Note'].apply(standardize_note_types)
    df.loc[df['LCH_UPN'].isin(nurse_list), 'Time_Note'] = 'Initial Evaluation'
    df.loc[df['LCH_UPN'].isin(alert_team_list), 'Time_Note'] = 'Alert'

    df['SharePoint_ID'] = pd.to_numeric(df['SharePoint_ID'], errors='coerce', downcast='integer')
    # Boolean column is flipped because it's stored differently in the database.
    df['Auto_Time'] = df['Auto_Time'].replace({True: 0, False: 1})
    df['Auto_Time'] = df['Auto_Time'].astype('Int64')
    df = df.rename(
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
    # Convert string Nan back to Null value.
    df.replace(r'(?i)^na(n)?$', None, regex=True, inplace=True)
    return df


def standardize_devices(df: pd.DataFrame) -> pd.DataFrame:
    df['Patient_ID'] = df['Patient_ID'].astype('Int64')
    df['Device_ID'] = df['Device_ID'].str.replace('-', '')
    df['Vendor'] = df.apply(standardize_vendor, axis=1)
    df = df.rename(
        columns={
            'Device_ID': 'model_number',
            'Device_Name': 'name',
            'Patient_ID': 'sharepoint_id'
        }
    )
    return df


def standardize_bp_readings(df: pd.DataFrame) -> pd.DataFrame:
    df['SharePoint_ID'] = df['SharePoint_ID'].astype('Int64')
    df['Manual_Reading'] = df['Manual_Reading'].replace({True: 1, False: 0})
    df['Manual_Reading'] = df['Manual_Reading'].astype('Int64')
    df['BP_Reading_Systolic'] = df['BP_Reading_Systolic'].astype(float).round(2)
    df['BP_Reading_Diastolic'] = df['BP_Reading_Diastolic'].astype(float).round(2)
    df = df.rename(
        columns={
            'SharePoint_ID': 'sharepoint_id',
            'Device_Model': 'temp_device',
            'Time_Recorded': 'recorded_datetime',
            'Time_Recieved': 'received_datetime',
            'BP_Reading_Systolic': 'systolic_reading',
            'BP_Reading_Diastolic': 'diastolic_reading',
            'Manual_Reading': 'is_manual'
        }
    )
    return df


def standardize_bg_readings(df: pd.DataFrame) -> pd.DataFrame:
    df['SharePoint_ID'] = df['SharePoint_ID'].astype('Int64')
    df['Manual_Reading'] = df['Manual_Reading'].replace({True: 1, False: 0})
    df['Manual_Reading'] = df['Manual_Reading'].astype('Int64')
    df['BG_Reading'] = df['BG_Reading'].astype(float).round(2)
    df = df.rename(
        columns={
            'SharePoint_ID': 'sharepoint_id',
            'Device_Model': 'temp_device',
            'Time_Recorded': 'recorded_datetime',
            'Time_Recieved': 'received_datetime',
            'BG_Reading': 'glucose_reading',
            'Manual_Reading': 'is_manual'
        }
    )
    return df


def patient_check_failed_data(df: pd.DataFrame) -> pd.DataFrame:
    failed_df = df[df['phone_number'].apply(lambda x: len(str(x)) != 10)]
    failed_df.loc[failed_df['phone_number'].apply(lambda x: len(str(x)) != 10), 'error_type'] = 'phone number length error'
    failed_df = df[df['social_security'].apply(lambda x: len(str(x)) != 9)]
    failed_df.loc[failed_df['social_security'].apply(lambda x: len(str(x)) != 9), 'error_type'] = 'social security length error'
    failed_df = df[df['zipcode'].apply(lambda x: len(str(x)) != 5)]
    failed_df.loc[failed_df['zipcode'].apply(lambda x: len(str(x)) != 5), 'error_type'] = 'zipcode length error'
    failed_df = df[df['medicare_beneficiary_id'].apply(lambda x: len(str(x)) != 11)]
    failed_df.loc[failed_df['medicare_beneficiary_id'].apply(lambda x: len(str(x)) != 11), 'error_type'] = 'medicare beneficiary id length error'
    failed_df = df[df['primary_payer_id'].apply(lambda x: len(str(x)) <= 30)]
    failed_df.loc[failed_df['primary_payer_id'].apply(lambda x: len(str(x)) <= 30), 'error_type'] = 'primary payer id length error'
    failed_df = df[df['secondary_payer_id'].apply(lambda x: len(str(x)) <= 30)]
    failed_df.loc[failed_df['secondary_payer_id'].apply(lambda x: len(str(x)) <= 30), 'error_type'] = 'secondary payer id length error'
    failed_df = df[df[['primary_payer_id', 'primary_payer_name']].isnull().all(axis=1)]
    failed_df.loc[failed_df[['primary_payer_name', 'primary_payer_id']].isnull().all(axis=1), 'error_type'] = 'missing insurance information'
    duplicate_df = df[df.duplicated(subset=['first_name', 'last_name', 'date_of_birth'], keep=False)]
    duplicate_df['error_type'] = 'duplicate patient'
    duplicate_df.sort_values(by=['first_name', 'last_name'])
    failed_df = pd.concat([failed_df, duplicate_df])
    failed_df.insert(0, 'error_type', failed_df.pop('error_type'))
    return failed_df


def patient_check_db_constraints(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['phone_number'].apply(lambda x: len(str(x)) <= 10)]
    df = df[df['social_security'].apply(lambda x: len(str(x)) <= 9)]
    df = df[df['zipcode'].apply(lambda x: len(str(x)) <= 5)]
    df = df[df['medicare_beneficiary_id'].apply(lambda x: len(str(x)) <= 11)]
    df = df[df['primary_payer_id'].apply(lambda x: len(str(x)) <= 30)]
    df = df[df['secondary_payer_id'].apply(lambda x: len(str(x)) <= 30)]
    return df


def add_id_col(df: pd.DataFrame, id_df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Merge pandas dataframes on specified column. Remove specified column after merge.

    Args:
        - df (pandas.DataFrame): Target dataframe requiring ID column.
        - id_df (pandas.DataFrame): ID dataframe containing ID column.
        - col (string): Column name to be merged and deleted.

    Returns:
        - pandas.Series: Target dataframe with newly added ID column.
    """
    df = pd.merge(df, id_df, on=col)
    df.drop(columns=[col], inplace=True)
    return df