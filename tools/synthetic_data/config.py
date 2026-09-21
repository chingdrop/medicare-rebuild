"""Fixed configuration for the synthetic data generator.

Everything here is invented. Nothing is derived from real people, places or payers.
"""

from datetime import datetime

GENERATOR_VERSION = 1
DEFAULT_SEED = 20250228
DEFAULT_PATIENTS = 200

# Same windows the pipeline's main() uses, so the demo mirrors the real entry point.
IMPORT_START = datetime(2025, 1, 1)
IMPORT_END = datetime(2025, 2, 28)
REPORT_START = datetime(2025, 2, 1)
REPORT_END = datetime(2025, 2, 28)

SYNTHETIC_MARKER = "SYNTHETIC"
FIRST_PATIENT_ID = 1001
ORPHAN_PATIENT_ID = 9001

# Staff. The AZURE_UPN-style names are the ones the pipeline special-cases when
# typing notes (see normalize_patient_notes), so the demo has to include them.
COACHES = ["Coach Alpha", "Coach Bravo", "Coach Charlie"]
NURSE_PRACTITIONER = "NursePractitioner"
REGISTERED_NURSE = "RegisteredNurse1"
ALERT_MEMBER = "AlertTeamMember1"
SERVICE_ACCOUNT = "svc_helpdesk"
STAFF = [
    *COACHES,
    NURSE_PRACTITIONER,
    REGISTERED_NURSE,
    "RegisteredNurse2",
    ALERT_MEMBER,
    SERVICE_ACCOUNT,
]

# Fixed fictional addresses. Single-word cities because the pipeline strips spaces
# from city names. ZIP codes use the unassigned 000xx range.
ADDRESSES = [
    ("100 Example Street", "Exampleton", "000"),
    ("215 Sample Avenue", "Sampleburg", "000"),
    ("42 Placeholder Road", "Demoville", "000"),
    ("7 Testing Lane", "Testfield", "000"),
    ("980 Mockingbird Court", "Fakeford", "000"),
    ("31 Synthetic Way", "Exampleton", "000"),
    ("560 Sandbox Boulevard", "Sampleburg", "000"),
    ("18 Dummy Drive", "Demoville", "000"),
    ("2044 Fixture Place", "Testfield", "000"),
    ("77 Notreal Terrace", "Fakeford", "000"),
    ("640 Stand-In Street", "Exampleton", "000"),
    ("9 Mockup Circle", "Sampleburg", "000"),
]
# Real state names on purpose: the pipeline resolves names to two-letter codes.
STATE_NAMES = {
    "California": "CA",
    "Texas": "TX",
    "Florida": "FL",
    "Ohio": "OH",
    "New York": "NY",
    "Arizona": "AZ",
}

# 555-01xx is the range reserved for fictional use; the area code is arbitrary.
AREA_CODES = ["212", "312", "415", "617", "202", "305"]

DX_CODES = ["E11.9", "E11.65", "I10", "I11.9", "E11.22", "R73.03"]

COMMERCIAL_PLANS = [
    "Sample Benefit Trust",
    "Demo Coverage Group",
    "Example Care Network",
]
SUPPLEMENT_PLANS = ["Placeholder Supplemental Trust", "Sandbox Supplement Fund"]

EMERGENCY_RELATIONSHIPS = ["Daughter", "Son", "Sister", "Brother", "Friend", "Neighbor"]

NOTE_TEMPLATES = [
    "Reviewed recent readings with the patient; no concerns raised.",
    "Patient reported taking medications as prescribed.",
    "Discussed device placement and reminded patient to take daily readings.",
    "Follow-up call to confirm the patient received supplies.",
    "Patient asked about scheduling; routed to the care team.",
]
NOTE_TEMPLATES_HTML = [
    "<p>Patient &amp; caregiver reviewed the care plan.</p>",
    "<div>Reviewed readings; patient feels well.</div>",
]

# note_type lookup rows the demo schema seeds; Time_Log.Notes uses these names.
NOTE_TYPES = [
    "Initial Evaluation",
    "Alert",
    "Follow-Up",
    "Care Coordination",
    "Monthly Review",
]
PATIENT_STATUSES = ["Active", "Inactive", "Onboard", "Do Not Call"]
VENDORS = ["Tenovi", "Omron"]
BILLING_CODES = ["99202", "99453", "99454", "99457", "99458"]
