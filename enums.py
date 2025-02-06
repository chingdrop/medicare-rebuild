state_abbreviations = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "American Samoa": "AS",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Guam": "GU",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Northern Mariana Islands": "MP",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Puerto Rico": "PR",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Trust Territories": "TT",
    "Utah": "UT",
    "Vermont": "VT",
    "Virgin Islands": "VI",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY"
}


# Key word lists must have all elements match in the target string.
# Spelling errors and abbreviations are added to increase matching.
insurance_keywords = {
    "AARP-PPO": [
        ["aarp"]
    ],
    "Aetna Insurance": [
        ["aetna"]
    ],
    "Ambetter of Arkansas": [
        ["ambetter", "arkansas"]
    ],
    "American Continental Insurance": [
        ["american", "continental"]
    ],
    "Amerigroup": [
        ["amerigroup"],
        ["amerigrp"]
    ],
    "Bankers Life And Casualty": [
        ["bankers", "life"]
    ],
    "BCBS Federal": [
        ["blue", "cross", "federal"],
        ["blue", "cross", "fed"],
        ["bcbs", "federal"],
        ["bcbs", "fed"]
    ],
    "Blue Cross and Blue Shield of Florida": [
        ["blue", "cross"],
        ["blue", "shield"],
        ["bluecross"],
        ["bcbs"],
        ["anthem"]
    ],
    "Care Improvement Plus": [
        ["care", "improvement"]
    ],
    "Care Plus": [
        ["care", "plus"],
        ["careplus"]
    ],
    "Caresource": [
        ["care", "source"],
        ["caresource"]
    ],
    "Central California Alliance for Health": [
        ["central", "california"],
        ["central", "ca"]
    ],
    "Champ Va": [
        ["champ", "va"]
    ],
    "Cigna": [
        ["cigna"]
    ],
    "Colonial Penn Life Insurance CO": [
        ["colonial", "penn"]
    ],
    "Commonwealth Care Alliance": [
        ["commonwealth", "care"],
        ["commonwealth", "health"]
    ],
    "Community Health Plan": [
        ["community", "health"]
    ],
    "CSI Medicare Supplement": [
        ["csi", "medicare"]
    ],
    "Devoted Health": [
        ["devoted"]
    ],
    "Emblem Health": [
        ["emblem"]
    ],
    "Fidelis Care New York": [
        ["fidelis"]
    ],
    "Freedom Health": [
        ["freedom"]
    ],
    "GEHA": [
        ["geha"]
    ],
    "Great southern life insurance": [
        ["great", "southern"]
    ],
    "Harvard Pilgrim Health Plan": [
        ["harvard", "pilgrim"]
    ],
    "Healthfirst Health Plan": [
        ["healthfirst"]
    ],
    "Humana": [
        ["humana"],
        ["humann"],
        ["humanna"]
    ],
    "Inland Empire Health Plan": [
        ["inland", "empire"]
    ],
    "Kaiser": [
        ["kaiser"],
        ["keiser"]
    ],
    "Kern Health Systems": [
        ["kern", "health"],
        ["kern", "family"]
    ],
    "Medicaid of California": [
        ["medicaid", "california"],
        ["medicaid", "ca"]
    ],
    "Medicaid of Florida": [
        ["medicaid", "florida"],
        ["medicaid", "fl"]
    ],
    "Medical Mutual of Ohio": [
        ["medical", "mutual", "ohio"],
        ["medical", "mutual", "oh"]
    ],
    "Meridian Health Plan": [
        ["meridian", "health"]
    ],
    "Molina Healthcare": [
        ["molina"]
    ],
    "Mutual of Omaha": [
        ["mutual", "omaha"]
    ],
    "Preferred Care Partners": [
        ["preferred", "care", "partner"]
    ],
    "Sunshine state Health Plan": [
        ["sunshine", "health"],
        ["sunshine", "insurance"]
    ],
    "Transamerica Life Insurance Co": [
        ["trans", "america"]
    ],
    "Tricare for Life": [
        ["tricare"]
    ],
    "United American":[
        ["united", "american"]
    ],
    "United HealthCare": [
        ["united", "health"],
        ["united", "healthcare"],
        ["united", "health", "care"],
        ["uhc"]
    ],
    "UPMC Health Plan": [
        ["upmc"]
    ],
    "Wellcare": [
        ["wellcare"]
    ],
    "Wellpoint": [
        ["wellpoint"]
    ],
    "Western Health Advantage": [
        ["western", "health", "advantage"]
    ],
    "Western United Life Assurance": [
        ["western", "united"]
    ]
}