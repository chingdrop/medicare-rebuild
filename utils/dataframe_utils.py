import re
import html
import pandas as pd
import numpy as np

from utils.enums import insurance_keywords, state_abbreviations, relationship_keywords, \
    race_keywords


def keyword_search(value: str, keywords: dict, keep_original=False) -> str:
    """Searches for a keyword being present in the value.
    
    Args:
        value (str): The value to be standardized.
        keywords (dict): The keyword dictionary, holding the desired name and the keyword.
        keep_original (bool): Option for keeping the original value provided.

    Returns:
        str: The standardized name, found as the key in the keyword list. 
    """
    for standard_name, keyword in keywords.items():
        if re.search(r'\b' + re.escape(keyword) + r'\b', value.lower()):
            return standard_name
    return value if keep_original else np.nan


def keyword_list_search(value: str, keywords: dict, keep_original=False) -> str:
    """Searches for a keyword being present in the value. Keywords are stored in lists of lists.
    All keywords must be present in the value to be considered True.
    
    Args:
        value (str): The value to be standardized.
        keywords (dict): The keyword dictionary, holding the desired name and the keyword.
        keep_original (bool): Option for keeping the original value provided.

    Returns:
        str: The standardized name, found as the key in the keyword list.
    """
    for standard_name, keyword_sets in keywords.items():
        for keyword_set in keyword_sets:
            if all(re.search(r'\b' + re.escape(keyword) + r'\b', value.lower()) for keyword in keyword_set):
                return standard_name
    return value if keep_original else np.nan


def extract_regex_pattern(value: str, pattern: re.Pattern, keep_original=False) -> str:
    """Searches the value for a matching regex pattern.
    
    Args:
        value (str): The value to be standardized.
        pattern (re.Pattern): The regex pattern object.
        keep_original (bool): Option for keeping the original value provided.

    Returns:
        str: The entire matching group of the regex pattern.
    """
    match = re.search(pattern, value)
    if match:
        return match.group(0)
    return value if keep_original else np.nan


def standardize_name(name: str, pattern: str) -> str:
    """Standardizes strings from name-like texts.
    Trims whitespace and titles the text. Flattens remaining whitespace to one space.
    Uses inverse regex pattern to replace all unmatched characters with empty string.

    Args:
        name (str): The value to be standardized.
        pattern (str): The inverse pattern used in replacing unwanted characters. 

    Returns:
        str: The standardized name text.    
    """
    name = str(name).strip().title()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(pattern, '', name)
    return name


def standardize_email(email: str) -> str:
    """Standardizes email address strings.
    Trims whitespace and lowers the text. Regex matching attempts to find an email address and extracts it.

    Args:
        email (str): The value to be standardized.

    Returns:
        str: The standardized email address.    
    """
    email = str(email).strip().lower()
    email_pattern = r'(^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$)'
    return extract_regex_pattern(email, email_pattern)


def standardize_state(state: str) -> str:
    """Standardizes US state strings. Trims whitespace and titles the text. 
    Searches state's name and correlates that with the State's two letter abbreviation.

    Args:
        state (str): The value to be standardized.

    Returns:
        str: The standardized US state abbreviation.    
    """
    state = str(state).strip().title()
    return keyword_search(state, state_abbreviations, keep_original=True).upper()


def standardize_mbi(mbi: str) -> str:
    """Standardizes medicare beneficiary ID strings. Trims whitespace and lowers the text. 
    Regex matching attempts to find a medicare beneficiary ID and extracts it.

    Args:
        mbi (str): The value to be standardized.

    Returns:
        str: The standardized medicare beneficiary ID.    
    """
    mbi = str(mbi).strip().upper()
    mbi_pattern = r'([A-Z0-9]{11})'
    return extract_regex_pattern(mbi, mbi_pattern, keep_original=True)


def standardize_dx_code(dx_code: str) -> str:
    """Standardizes diagnosis codes.
    Trims whitespace and uppers the text. Searches text for regex pattern of diagnosis code. 
    Joins all elements into a single string with commas as the separator. 

    Args:
        dx_code (str): The value to be standardized.

    Returns:
        str: string representation of the list of Dx codes.
    """
    dx_code = str(dx_code).strip().upper()
    matches = re.finditer(r'[E|I|R]\d+(\.\d+)?', dx_code)
    matches = [match.group(0).replace('.', '') for match in matches]
    return ','.join(matches)


def standardize_insurance_name(name: str) -> str:
    """Standardizes insurance name strings. Trims whitespace and titles the text.
    Searches the insurance name for keywords and correlates that with a list of standard insurance names.

    Args:
        name (str): The value to be standardized.

    Returns:
        str: The standardized insurance name.    
    """
    name = str(name).strip().title()
    return keyword_list_search(name, insurance_keywords, keep_original=True)


def standardize_insurance_id(ins_id: str) -> str:
    """Standardizes insurance ID strings. Trims whitespace and uppers the text.
    Any non-alphanumeric character is replaced with empty string.
    Regex matching attempts to find insurance IDs and extracts them.

    Args:
        ins_id (str): The value to be standardized.

    Returns:
        str: The standardized insurance ID.    
    """
    ins_id = str(ins_id).strip().upper()
    ins_id = re.sub(r'[^A-Z0-9]', '', ins_id)
    id_pattern = r'([A-Z]*\d+[A-Z]*\d+[A-Z]*\d+[A-Z]*\d*)'
    return extract_regex_pattern(ins_id, id_pattern, keep_original=True)


def fill_primary_payer(row: pd.Series) -> pd.Series:
    """Fills primary payer name with 'Medicare Part B'.
    If insurance name and insurance ID is null; and medicare beneficiary ID is not null.
    Then the primary payer name gets filled with 'Medicare Part B'.

    Args:
        row (pandas.Series): Row of Dataframe to be standardized.

    Returns:
        pandas.Series: The standardized row of Dataframe.    
    """
    row.replace(r'(?i)^nan$', np.nan, regex=True, inplace=True)
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
    row.replace(r'(?i)^nan$', np.nan, regex=True, inplace=True)
    if row['Insurance Name:'] == 'Medicare Part B' and pd.isnull(row['Insurance ID:']) :
        return row['Medicare ID number']
    return row['Insurance ID:']


def standardize_call_time(call_time) -> int:
    """Standardizes call time in seconds.
    Converts value to a timedelta object and then calculates total seconds.

    Args:
        call_time: The value to be standardized.

    Returns:
        int: The standardized call time in seconds.    
    """
    if not call_time:
        return 0
    return pd.to_timedelta(str(call_time)).total_seconds()


def standardize_note_types(note_type: str) -> str:
    """Standardizes note type value.
    Replaces common phrase for proper note type. Split note type by commas, then use the first element.

    Args:
        note_type (str): The value to be standardized.

    Returns:
        str: The standardized note type phrase.    
    """
    if note_type == 'Initial Evaluation with APRN':
        note_type = 'Initial Evaluation'
    return str(note_type).split(',')[0]


def standardize_vendor(row: pd.Series) -> pd.Series:
    """Original values had Vendor name in the Device name.
    If Vendor name is a substring of Device name then return the Vendor name.
    Else, if 'Tenvoi' or 'Omron' is in Device name then return 'Tenvoi' or 'Omron' respectively.

    Args:
        row (pandas.Series): Row of Dataframe to be standardized.

    Returns:
        pandas.Series: The standardized row of Dataframe.    
    """
    if not row['Vendor'] in row['Device_Name']:
        if 'Tenovi' in row['Device_Name']:
            return 'Tenovi'
        else:
            return 'Omron'
    return row['Vendor']


def standardize_emcontact_relationship(name: str) -> str:
    """Standardizes emergency contact relationships.
    Trims whitespace and titles the text.
    Searches the emergency contact name for keywords and correlates that with a list of standard relationship names.

    Args:
        name (str): The value to be standardized.

    Returns:
        str: The standardized relatioship name. Or None
    """
    name = str(name).strip().title()
    return keyword_search(name, relationship_keywords)


def standardize_race(race: str) -> str:
    """Standardizes patient race strings.
    Trims whitespace and titles the text.
    Searches the race for keywords and correlates that with a list of standard races.
    Keywords are held in lists where all elements of the list must be present for a positive match.

    Args:
        name (str): The value to be standardized.

    Returns:
        str: The standardized race.    
    """
    race = str(race).strip().title()
    return keyword_list_search(race, race_keywords, keep_original=True)


def standardize_weight(weight: str) -> str:
    """Standardizes patient weight strings. Search values for common characters indicating height and remove them.
    Remove all characters that aren't numeric. Check if weight string has more than 3 characters and trim values to only 3 characers.

    Args:
        weight (str): The value to be standardized.

    Returns:
        int: The standardized weight in pounds.    
    """
    weight = str(weight).strip()
    height_chars = ["'", '"', 'ft', 'in']
    if any(char in weight.lower() for char in height_chars):
        return np.nan
    weight = re.sub(r'\D', '', weight)
    if len(weight) > 3:
        weight = weight[:3]
    return int(weight) if weight else 0


def standardize_height(height: str) -> str:
    """Standardizes patient height strings. Search values for common characters indicating weight and remove them.
    Search values for strings that indicate height: 5ft2, 5'2", etc. Get the feet and inch values and converts to inches.

    Args:
        height (str): The value to be standardized.

    Returns:
        int: The standardized height in inches.    
    """
    height = str(height).strip()
    weight_chars = ['lbs', 'kg']
    if any(char in height.lower() for char in weight_chars):
        return np.nan
    match = re.match(r'^(\d+)[\D]*?(\d+)?[\D]*?$', height)
    if match:
        feet = int(match.group(1))
        inches = int(match.group(2)) if match.group(2) else 0
        return (feet * 12) + inches
    

def normalize_users(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['givenName', 'surname', 'displayName', 'mail', 'id']]
    df = df.rename(columns={
        'givenName': 'first_name',
        'surname': 'last_name',
        'displayName': 'display_name',
        'mail': 'email',
        'id': 'ms_entra_id'
    })
    return df


def normalize_patients(df: pd.DataFrame) -> pd.DataFrame:
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
    df['Race'] = df['Race'].apply(standardize_race)
    df['Weight'] = df['Weight'].apply(standardize_weight)
    df['Height'] = df['Height'].apply(standardize_height)

    # The logic in standardize name can be used for address text as well.
    df['Mailing Address'] = df['Mailing Address'].apply(standardize_name, args=(r'[^a-zA-Z0-9\s#.-/]',))
    df['City'] = df['City'].apply(standardize_name, args=(r'[^a-zA-Z-]',))
    df['State'] = df['State'].apply(standardize_state)
    df['Zip code'] = df['Zip code'].astype(str).str.split('-', n=1).str[0]

    df['EmergencyRelationship'] = df['EmergencyName'].apply(standardize_emcontact_relationship)
    df['EmergencyRelationship2'] = df['EmergencyName2'].apply(standardize_emcontact_relationship)
    df['EmergencyName'] = df['EmergencyName'].apply(standardize_name, args=(r'[^a-zA-Z\s.-/()]',))
    df['EmergencyNumber'] = df['EmergencyNumber'].astype(str).str.replace(r'\D', '', regex=True)
    df['EmergencyName2'] = df['EmergencyName2'].apply(standardize_name, args=(r'[^a-zA-Z\s.-/()]',))
    df['EmergencyNumber2'] = df['EmergencyNumber2'].astype(str).str.replace(r'\D', '', regex=True)

    df['Medicare ID number'] = df['Medicare ID number'].apply(standardize_mbi)
    df['DX_Code'] = df['DX_Code'].apply(standardize_dx_code)
    df['Insurance ID:'] = df['Insurance ID:'].apply(standardize_insurance_id)
    df['InsuranceID2'] = df['InsuranceID2'].apply(standardize_insurance_id)
    df['Insurance Name:'] = df['Insurance Name:'].apply(standardize_insurance_name)
    df['InsuranceName2'] = df['InsuranceName2'].apply(standardize_insurance_name)
    df['Insurance Name:'] = df.apply(fill_primary_payer, axis=1)
    df['Insurance ID:'] = df.apply(fill_primary_payer_id, axis=1)

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
            'Race': 'temp_race',
            'Relationship_Status': 'temp_marital_status',
            'Preferred_Language': 'preferred_language',
            'Weight': 'weight_lbs',
            'Height': 'height_in',
            'ID': 'sharepoint_id',
            'Mailing Address': 'street_address',
            'City': 'city',
            'State': 'temp_state',
            'Zip code': 'zipcode',
            'EmergencyName': 'emergency_full_name',
            'EmergencyNumber': 'emergency_phone_number',
            'EmergencyRelationship': 'emergency_relationship',
            'EmergencyName2': 'emergency_full_name2',
            'EmergencyNumber2': 'emergency_phone_number2',
            'EmergencyRelationship2': 'emergency_relationship2',
            'Medicare ID number': 'medicare_beneficiary_id',
            'Insurance ID:': 'primary_payer_id',
            'Insurance Name:': 'primary_payer_name',
            'InsuranceID2': 'secondary_payer_id',
            'InsuranceName2': 'secondary_payer_name',
            'On-board Date': 'evaluation_datetime',
            'DX_Code': 'temp_dx_code',
            'Member_Status': 'temp_status_type',
            'Health Coach': 'temp_user'
        }
    )
    # Convert string Nan back to Null value.
    df.replace(r'(?i)^nan$', None, regex=True, inplace=True)
    return df


def create_patient_df(df: pd.DataFrame) -> pd.DataFrame:
    return df[[
        'first_name',
        'last_name',
        'middle_name',
        'name_suffix',
        'full_name',
        'nick_name',
        'date_of_birth',
        'sex',
        'email',
        'phone_number',
        'social_security',
        'temp_race',
        'temp_marital_status',
        'preferred_language',
        'weight_lbs',
        'height_in',
        'sharepoint_id',
        'temp_user'
    ]]


def create_patient_address_df(df: pd.DataFrame) -> pd.DataFrame:
    return df[['street_address', 'city', 'temp_state', 'zipcode', 'sharepoint_id']]


def create_patient_insurance_df(df: pd.DataFrame) -> pd.DataFrame:
    return df[[
        'medicare_beneficiary_id',
        'primary_payer_id',
        'primary_payer_name',
        'secondary_payer_id',
        'secondary_payer_name',
        'sharepoint_id'
    ]]


def create_med_necessity_df(df: pd.DataFrame) -> pd.DataFrame:
    med_nec_df = df[['evaluation_datetime', 'temp_dx_code', 'sharepoint_id']]
    med_nec_df.loc[:, 'temp_dx_code'] = med_nec_df['temp_dx_code'].str.split(',')
    med_nec_df = med_nec_df.explode('temp_dx_code', ignore_index=True)
    return med_nec_df


def create_patient_status_df(df: pd.DataFrame) -> pd.DataFrame:
    patient_status_df = df[['temp_status_type', 'sharepoint_id']]
    patient_status_df['modified_date'] = pd.Timestamp.now()
    patient_status_df['temp_user'] = 'ITHelp'
    return patient_status_df


def create_emcontacts_df(df: pd.DataFrame) -> pd.DataFrame:
    emcontacts_df1 = df[[
        'emergency_full_name',
        'emergency_phone_number',
        'emergency_relationship',
        'sharepoint_id'
    ]]
    emcontacts_df1 = emcontacts_df1.rename(columns={
        'emergency_full_name': 'full_name',
        'emergency_phone_number': 'phone_number',
        'emergency_relationship': 'relationship'
    })
    emcontacts_df2 = df[[
        'emergency_full_name2',
        'emergency_phone_number2',
        'emergency_relationship2',
        'sharepoint_id'
    ]]
    emcontacts_df2 = emcontacts_df2.rename(columns={
        'emergency_full_name2': 'full_name',
        'emergency_phone_number2': 'phone_number',
        'emergency_relationship2': 'relationship'
    })
    emcontacts_df = pd.concat([emcontacts_df1, emcontacts_df2])
    emcontacts_df = emcontacts_df.dropna(subset=['full_name', 'phone_number'])
    return emcontacts_df


def normalize_patient_notes(df: pd.DataFrame) -> pd.DataFrame:
    df['Recording_Time'] = df['Recording_Time'].apply(standardize_call_time)
    df.loc[df['LCH_UPN'].isin(['Joycelynn Harris']), 'Recording_Time'] = 900

    df['Notes'] = df['Notes'].apply(html.unescape)
    df['Notes'] = df['Notes'].str.replace(r'<.*?>', '', regex=True)

    df['Time_Note'] = df['Time_Note'].apply(standardize_note_types)
    df.loc[df['LCH_UPN'].isin(['Joycelynn Harris', 'Melanie Coffey', 'Krista Lewin']), 'Time_Note'] = 'Initial Evaluation'
    df.loc[df['LCH_UPN'].isin(['Christylyn Diosma', 'Maria Albingco', 'Mary Cortes', 'Richel Rodriguez', 'Rigel Sagayno']), 'Time_Note'] = 'Alert'

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


def normalize_devices(df: pd.DataFrame) -> pd.DataFrame:
    df['Patient_ID'] = df['Patient_ID'].astype('Int64')
    df['Device_ID'] = df['Device_ID'].str.replace('-', '')
    df['Vendor'] = df.apply(standardize_vendor, axis=1)
    df = df.rename(
        columns={
            'Device_ID': 'hardware_uuid',
            'Device_Name': 'name',
            'Patient_ID': 'sharepoint_id'
        }
    )
    return df


def normalize_bp_readings(df: pd.DataFrame) -> pd.DataFrame:
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


def normalize_bg_readings(df: pd.DataFrame) -> pd.DataFrame:
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


# def patient_check_failed_data(df: pd.DataFrame) -> pd.DataFrame:
#     failed_df = df[df['phone_number'].apply(lambda x: len(str(x)) != 11)]
#     failed_df.loc[failed_df['phone_number'].apply(lambda x: len(str(x)) != 11), 'error_type'] = 'phone number length error'
#     failed_df = df[df['social_security'].apply(lambda x: len(str(x)) != 9)]
#     failed_df.loc[failed_df['social_security'].apply(lambda x: len(str(x)) != 9), 'error_type'] = 'social security length error'
#     failed_df = df[df['zipcode'].apply(lambda x: len(str(x)) != 5)]
#     failed_df.loc[failed_df['zipcode'].apply(lambda x: len(str(x)) != 5), 'error_type'] = 'zipcode length error'
#     failed_df = df[df['emergency_phone_number'].apply(lambda x: len(str(x)) != 11)]
#     failed_df.loc[failed_df['emergency_phone_number'].apply(lambda x: len(str(x)) != 11), 'error_type'] = 'phone number length error'
#     failed_df = df[df['emergency_phone_number2'].apply(lambda x: len(str(x)) != 11)]
#     failed_df.loc[failed_df['emergency_phone_number2'].apply(lambda x: len(str(x)) != 11), 'error_type'] = 'phone number length error'
#     failed_df = df[df['medicare_beneficiary_id'].apply(lambda x: len(str(x)) != 11)]
#     failed_df.loc[failed_df['medicare_beneficiary_id'].apply(lambda x: len(str(x)) != 11), 'error_type'] = 'medicare beneficiary id length error'
#     failed_df = df[df['primary_payer_id'].apply(lambda x: len(str(x)) >= 30)]
#     failed_df.loc[failed_df['primary_payer_id'].apply(lambda x: len(str(x)) >= 30), 'error_type'] = 'primary payer id length error'
#     failed_df = df[df['secondary_payer_id'].apply(lambda x: len(str(x)) >= 30)]
#     failed_df.loc[failed_df['secondary_payer_id'].apply(lambda x: len(str(x)) >= 30), 'error_type'] = 'secondary payer id length error'
#     missing_df = df[df[['primary_payer_id', 'primary_payer_name']].isnull().all(axis=1)]
#     missing_df['error_type'] = 'missing insurance information'
#     duplicate_df = df[df.duplicated(subset=['first_name', 'last_name', 'date_of_birth'], keep=False)]
#     duplicate_df['error_type'] = 'duplicate patient'
#     duplicate_df.sort_values(by=['first_name', 'last_name'])
#     failed_df = pd.concat([failed_df, duplicate_df, missing_df])
#     failed_df.insert(0, 'error_type', failed_df.pop('error_type'))
#     return failed_df


def check_patient_db_constraints(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['phone_number'].apply(lambda x: len(str(x)) <= 11)]
    df = df[df['social_security'].apply(lambda x: len(str(x)) <= 9)]
    df = df[df['temp_state'].apply(lambda x: len(str(x)) <= 2)]
    df = df[df['zipcode'].apply(lambda x: len(str(x)) <= 5)]
    df = df[df['emergency_phone_number'].apply(lambda x: len(str(x)) <= 11)]
    df = df[df['emergency_phone_number2'].apply(lambda x: len(str(x)) <= 11)]
    df = df[df['medicare_beneficiary_id'].apply(lambda x: len(str(x)) <= 11)]
    df = df[df['primary_payer_id'].apply(lambda x: len(str(x)) <= 30)]
    df = df[df['secondary_payer_id'].apply(lambda x: len(str(x)) <= 30)]
    return df


def add_id_col(df: pd.DataFrame, id_df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Merge pandas dataframes on specified column. Remove specified column after merge.

    Args:
        df (pandas.DataFrame): Target dataframe requiring ID column.
        id_df (pandas.DataFrame): ID dataframe containing ID column.
        col (str): Column name to be merged and deleted.

    Returns:
        pandas.Series: Target dataframe with newly added ID column.
    """
    df = pd.merge(df, id_df, on=col)
    df.drop(columns=[col], inplace=True)
    return df