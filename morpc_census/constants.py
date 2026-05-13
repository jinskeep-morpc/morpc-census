"""
Domain lookup tables and sort orders for Census variable categories.

These are pure data constants — no network calls or class dependencies.
Import them directly from morpc_census or from this module.
"""

HIGHLEVEL_GROUP_DESC = {
    "01": "Sex, Age, and Population",
    "02": "Race",
    "03": "Ethnicity",
    "04": "Ancestry",
    "05": "Nativity and Citizenship",
    "06": "Place of Birth",
    "07": "Geographic Mobility",
    "08": "Transportation to Work",
    "09": "Children",
    "10": "Grandparents and Grandchildren",
    "11": "Household Type",
    "12": "Marriage and Marital Status",
    "13": "Mothers and Births",
    "14": "School Enrollment",
    "15": "Educational Attainment",
    "16": "Language Spoken at Home",
    "17": "Poverty",
    "18": "Disability",
    "19": "Household Income",
    "20": "Earnings",
    "21": "Veterans",
    "22": "Food Stamps/SNAP",
    "23": "Workers and Employment Status",
    "24": "Occupation, Industry, Class",
    "25": "Housing Units, Tenure, Housing Costs",
    "26": "Group Quarters",
    "27": "Health Insurance",
    "28": "Computers and Internet",
    "29": "Voting-Age",
    "98": "Coverage Rates and Allocation Rates",
    "99": "Allocations",
}

HIGHLEVEL_DESC_TO_ID = {v: k for k, v in HIGHLEVEL_GROUP_DESC.items()}

AGEGROUP_MAP = {
    'Under 5 years': 'Under 5 years',
    '5 to 9 years': '5 to 9 years',
    '10 to 14 years': '10 to 14 years',
    '15 to 17 years': '15 to 19 years',
    '18 and 19 years': '15 to 19 years',
    '20 years': '20 to 24 years',
    '21 years': '20 to 24 years',
    '22 to 24 years': '20 to 24 years',
    '25 to 29 years': '25 to 29 years',
    '30 to 34 years': '30 to 34 years',
    '35 to 39 years': '35 to 39 years',
    '40 to 44 years': '40 to 44 years',
    '45 to 49 years': '45 to 49 years',
    '50 to 54 years': '50 to 54 years',
    '55 to 59 years': '55 to 59 years',
    '60 and 61 years': '60 to 64 years',
    '62 to 64 years': '60 to 64 years',
    '65 and 66 years': '65 to 69 years',
    '67 to 69 years': '65 to 69 years',
    '70 to 74 years': '70 to 74 years',
    '75 to 79 years': '75 to 79 years',
    '80 to 84 years': '80 to 84 years',
    '85 years and over': '85 years and over',
}

AGEGROUP_SORT_ORDER = {
    'Total': 1,
    'Under 5 years': 2,
    '5 to 9 years': 3,
    '10 to 14 years': 4,
    '15 to 19 years': 5,
    '20 to 24 years': 6,
    '25 to 29 years': 7,
    '30 to 34 years': 8,
    '35 to 39 years': 9,
    '40 to 44 years': 10,
    '45 to 49 years': 11,
    '50 to 54 years': 12,
    '55 to 59 years': 13,
    '60 to 64 years': 14,
    '65 to 69 years': 15,
    '70 to 74 years': 16,
    '75 to 79 years': 17,
    '80 to 84 years': 18,
    '85 years and over': 19,
}

RACE_TABLE_MAP = {
    'A': 'White Alone',
    'B': 'Black or African American Alone',
    'C': 'American Indian and Alaska Native Alone',
    'D': 'Asian Alone',
    'E': 'Native Hawaiian and Other Pacific Islander Alone',
    'F': 'Some Other Race Alone',
    'G': 'Two or More Races',
    'H': 'White Alone, Not Hispanic or Latino',
    'I': 'Hispanic or Latino',
}

EDUCATION_ATTAIN_MAP = {
    'No schooling completed': 'No high school diploma',
    'Nursery school': 'No high school diploma',
    'Kindergarten': 'No high school diploma',
    '1st grade': 'No high school diploma',
    '2nd grade': 'No high school diploma',
    '3rd grade': 'No high school diploma',
    '4th grade': 'No high school diploma',
    '5th grade': 'No high school diploma',
    '6th grade': 'No high school diploma',
    '7th grade': 'No high school diploma',
    '8th grade': 'No high school diploma',
    '9th grade': 'No high school diploma',
    '10th grade': 'No high school diploma',
    '11th grade': 'No high school diploma',
    '12th grade, no diploma': 'No high school diploma',
    'Regular high school diploma': 'High school diploma or equivalent',
    'GED or alternative credential': 'High school diploma or equivalent',
    'Some college, less than 1 year': 'High school diploma or equivalent',
    'Some college, 1 or more years, no degree': 'High school diploma or equivalent',
    "Associate's degree": "Associate's degree",
    "Bachelor's degree": "Bachelor's degree",
    "Master's degree": "More than Bachelor's",
    'Professional school degree': "More than Bachelor's",
    'Doctorate degree': "More than Bachelor's",
}

EDUCATION_ATTAIN_SORT_ORDER = {
    'Total': 1,
    'No high school diploma': 2,
    'High school diploma or equivalent': 3,
    "Associate's degree": 4,
    "Bachelor's degree": 5,
    "More than Bachelor's": 6,
}

INCOME_TO_POVERTY_MAP = {
    'Under .50': 'Under .50',
    '.50 to .74': '.50 to .99',
    '.75 to .99': '.50 to .99',
    '1.00 to 1.24': '1.00 to 1.99',
    '1.25 to 1.49': '1.00 to 1.99',
    '1.50 to 1.74': '1.00 to 1.99',
    '1.75 to 1.84': '1.00 to 1.99',
    '1.85 to 1.99': '1.00 to 1.99',
    '2.00 to 2.99': '2.00 to 2.99',
    '3.00 to 3.99': '3.00 to 3.99',
    '4.00 to 4.99': '4.00 and over',
    '5.00 and over': '4.00 and over',
}

INCOME_TO_POVERTY_SORT_ORDER = {
    'Total': 0,
    'Under .50': 1,
    '.50 to .99': 2,
    '1.00 to 1.99': 3,
    '2.00 to 2.99': 4,
    '3.00 to 3.99': 5,
    '4.00 and over': 6,
}

NTD_AGEMAP = {
    'Under 5 years': '18 years and under',
    '5 to 9 years': '18 years and under',
    '10 to 14 years': '18 years and under',
    '15 to 17 years': '18 years and under',
    '18 and 19 years': '18 years and under',  # apportion half here, half in 19 years
    '20 years': '19 to 64 years',
    '21 years': '19 to 64 years',
    '22 to 24 years': '19 to 64 years',
    '25 to 29 years': '19 to 64 years',
    '30 to 34 years': '19 to 64 years',
    '35 to 39 years': '19 to 64 years',
    '40 to 44 years': '19 to 64 years',
    '45 to 49 years': '19 to 64 years',
    '50 to 54 years': '19 to 64 years',
    '55 to 59 years': '19 to 64 years',
    '60 and 61 years': '19 to 64 years',
    '62 to 64 years': '19 to 64 years',
    '65 and 66 years': '65 years and over',
    '67 to 69 years': '65 years and over',
    '70 to 74 years': '65 years and over',
    '75 to 79 years': '65 years and over',
    '80 to 84 years': '65 years and over',
    '85 years and over': '65 years and over',
}

NTD_AGEMAP_ORDER = {
    'Total': 0,
    '18 years and under': 1,
    '20 to 64 years': 2,
    '65 years and over': 3,
}
