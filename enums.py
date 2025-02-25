state_abbreviations = {
    'AL': 'Alabama',
    'AK': 'Alaska',
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'AS': 'American Samoa',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'DC': 'District of Columbia',
    'FL': 'Florida',
    'GA': 'Georgia',
    'GU': 'Guam',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NY': 'New York',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'MP': 'Northern Mariana Islands',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'PR': 'Puerto Rico',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'TT': 'Trust Territories',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VI': 'Virgin Islands',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming'
}

# Key word lists must have all elements match in the target string.
# Spelling errors and abbreviations are added to increase matching.
insurance_keywords = {
    'AARP-PPO': [
        ['aarp']
    ],
    'Aetna Insurance': [
        ['aetna']
    ],
    'Ambetter of Arkansas': [
        ['ambetter', 'arkansas']
    ],
    'American Continental Insurance': [
        ['american', 'continental']
    ],
    'Amerigroup': [
        ['amerigroup'],
        ['amerigrp']
    ],
    'Bankers Life And Casualty': [
        ['bankers', 'life']
    ],
    'BCBS Federal': [
        ['blue', 'cross', 'federal'],
        ['blue', 'cross', 'fed'],
        ['bcbs', 'federal'],
        ['bcbs', 'fed']
    ],
    'Blue Cross and Blue Shield of Florida': [
        ['blue', 'cross'],
        ['blue', 'shield'],
        ['bluecross'],
        ['bcbs'],
        ['anthem']
    ],
    'Care Improvement Plus': [
        ['care', 'improvement']
    ],
    'Care Plus': [
        ['care', 'plus'],
        ['careplus']
    ],
    'Caresource': [
        ['care', 'source'],
        ['caresource']
    ],
    'Central California Alliance for Health': [
        ['central', 'california'],
        ['central', 'ca']
    ],
    'Champ Va': [
        ['champ', 'va']
    ],
    'Cigna': [
        ['cigna']
    ],
    'Colonial Penn Life Insurance CO': [
        ['colonial', 'penn']
    ],
    'Commonwealth Care Alliance': [
        ['commonwealth', 'care'],
        ['commonwealth', 'health']
    ],
    'Community Health Plan': [
        ['community', 'health']
    ],
    'CSI Medicare Supplement': [
        ['csi', 'medicare']
    ],
    'Devoted Health': [
        ['devoted']
    ],
    'Emblem Health': [
        ['emblem']
    ],
    'Fidelis Care New York': [
        ['fidelis']
    ],
    'Freedom Health': [
        ['freedom']
    ],
    'GEHA': [
        ['geha']
    ],
    'Great southern life insurance': [
        ['great', 'southern']
    ],
    'Harvard Pilgrim Health Plan': [
        ['harvard', 'pilgrim']
    ],
    'Healthfirst Health Plan': [
        ['healthfirst']
    ],
    'Humana': [
        ['humana'],
        ['humann'],
        ['humanna']
    ],
    'Inland Empire Health Plan': [
        ['inland', 'empire']
    ],
    'Kaiser': [
        ['kaiser'],
        ['keiser']
    ],
    'Kern Health Systems': [
        ['kern', 'health'],
        ['kern', 'family']
    ],
    'Medicaid of California': [
        ['medicaid', 'california'],
        ['medicaid', 'ca']
    ],
    'Medicaid of Florida': [
        ['medicaid', 'florida'],
        ['medicaid', 'fl']
    ],
    'Medical Mutual of Ohio': [
        ['medical', 'mutual', 'ohio'],
        ['medical', 'mutual', 'oh']
    ],
    'Meridian Health Plan': [
        ['meridian', 'health']
    ],
    'Molina Healthcare': [
        ['molina']
    ],
    'Mutual of Omaha': [
        ['mutual', 'omaha']
    ],
    'Preferred Care Partners': [
        ['preferred', 'care', 'partner']
    ],
    'Sunshine state Health Plan': [
        ['sunshine', 'health'],
        ['sunshine', 'insurance']
    ],
    'Transamerica Life Insurance Co': [
        ['trans', 'america']
    ],
    'Tricare for Life': [
        ['tricare']
    ],
    'United American':[
        ['united', 'american']
    ],
    'United HealthCare': [
        ['united', 'health'],
        ['united', 'healthcare'],
        ['united', 'health', 'care'],
        ['uhc']
    ],
    'UPMC Health Plan': [
        ['upmc']
    ],
    'Wellcare': [
        ['wellcare']
    ],
    'Wellpoint': [
        ['wellpoint']
    ],
    'Western Health Advantage': [
        ['western', 'health', 'advantage']
    ],
    'Western United Life Assurance': [
        ['western', 'united']
    ]
}

relationship_keywords = {
    'Mother': 'mother',
    'Father': 'father',
    'Daughter': 'daughter',
    'Son': 'son',
    'Grandson': 'grandson',
    'Granddaughter': 'granddaughter',
    'Sister': 'sister',
    'Brother': 'brother',
    'Nephew': 'nephew',
    'Niece': 'niece',
    'Cousin': 'cousin',
    'Girlfriend': 'girlfriend',
    'Boyfriend': 'boyfriend',
    'Wife': 'wife',
    'Husband': 'husband',
    'Fiance': 'fiance',
    'Caregiver': 'caregiver',
    'Friend': 'friend',
    'Neighbor': 'neighbor'
}

race_keywords = {
    'Asian': [
        ['asian']
    ],
    'Black': [
        ['black'],
        ['african american']
    ],
    'Hispanic/Latin': [
        ['hispanic'],
        ['latin']
    ],
    'Middle Eastern': [
        ['middle', 'eastern']
    ],
    'Native American': [
        ['native', 'american'],
        ['alaska', 'native']
    ],
    'Pacific Islander': [
        ['pacific', 'islander'],
        ['native', 'hawaiian']
    ],
    'White': [
        ['white'],
        ['caucasian'],
    ]
}