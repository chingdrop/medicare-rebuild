"""Builds individual synthetic records. All randomness flows through one seeded
`random.Random` and one seeded Faker, so output depends only on the seed."""

import random
from datetime import date, datetime, timedelta

from faker import Faker

from tools.synthetic_data import config as cfg
from tools.synthetic_data.model import Device, Note, Reading

PATIENT_CSV_COLUMNS = [
    "First Name",
    "Last Name",
    "Middle Name",
    "Nickname",
    "Phone Number",
    "Gender",
    "Email",
    "Suffix",
    "Social Security",
    "Race",
    "Weight",
    "Height",
    "Mailing Address",
    "City",
    "State",
    "Zip code",
    "EmergencyName",
    "EmergencyNumber",
    "EmergencyName2",
    "EmergencyNumber2",
    "Medicare ID number",
    "DX_Code",
    "Insurance ID:",
    "Insurance Name:",
    "InsuranceID2",
    "InsuranceName2",
    "On-board Date",
    "Member_Status",
    "Health Coach",
    "Relationship_Status",
    "Preferred_Language",
    "DOB",
    "ID",
]


def day(month: int, dom: int) -> date:
    """A date in the demo's 2025 billing period."""
    return date(2025, month, dom)


def days_between(first: date, count: int) -> list[date]:
    return [first + timedelta(days=i) for i in range(count)]


class Factory:
    def __init__(self, seed: int):
        self.rng = random.Random(seed)
        self.fake = Faker("en_US")
        self.fake.seed_instance(seed)

    # -- patients ---------------------------------------------------------

    def phone(self) -> str:
        area = self.rng.choice(cfg.AREA_CODES)
        return f"({area}) 555-01{self.rng.randint(0, 99):02d}"

    def patient_row(self, sid: int, *, contacts: int = 1) -> dict:
        rng, fake = self.rng, self.fake
        female = rng.random() < 0.55
        first = fake.first_name_female() if female else fake.first_name_male()
        last = fake.last_name()
        middle = rng.choice(["", "", "A", "M", "J", "Lee", "Rose"])
        street, city, zip_prefix = rng.choice(cfg.ADDRESSES)
        state = rng.choice(list(cfg.STATE_NAMES) + list(cfg.STATE_NAMES.values()))
        age = rng.choice([rng.randint(66, 89)] * 9 + [rng.randint(42, 64)])
        dob = date(2025 - age, rng.randint(1, 12), rng.randint(1, 28))
        dx = ", ".join(rng.sample(cfg.DX_CODES, rng.choice([1, 2, 2, 3])))
        commercial = rng.random() < 0.3
        supplement = rng.random() < 0.15
        status = rng.choices(
            ["Active", "In-Active", "On-Board", "DO NOT CALL"], [80, 8, 8, 4]
        )[0]
        local = f"{first}.{last}".lower()
        local = "".join(ch for ch in local if ch.isalnum() or ch == ".")
        row = {
            "First Name": first,
            "Last Name": last,
            "Middle Name": middle,
            "Nickname": f"{cfg.SYNTHETIC_MARKER}-{sid:06d}",
            "Phone Number": self.phone(),
            "Gender": "Female" if female else "Male",
            "Email": f"{local}.syn{sid}@example.com",
            "Suffix": "" if female else rng.choice(["", "", "", "Jr", "Sr"]),
            "Social Security": "",
            "Race": rng.choice(
                ["White", "Black", "Asian", "Hispanic", "Caucasian", "Native American"]
            ),
            "Weight": f"{rng.randint(120, 260)} lbs",
            "Height": self._height(),
            "Mailing Address": street,
            "City": city,
            "State": state,
            "Zip code": f"{zip_prefix}{rng.randint(10, 99)}",
            "EmergencyName": self._contact_name(),
            "EmergencyNumber": self.phone(),
            "EmergencyName2": self._contact_name() if contacts > 1 else "",
            "EmergencyNumber2": self.phone() if contacts > 1 else "",
            "Medicare ID number": f"SYN-{sid:06d}",
            "DX_Code": dx,
            "Insurance ID:": f"SYNINS-{rng.randint(100000, 999999)}"
            if commercial
            else "",
            "Insurance Name:": rng.choice(cfg.COMMERCIAL_PLANS) if commercial else "",
            "InsuranceID2": f"SYNSUP-{rng.randint(100000, 999999)}"
            if supplement
            else "",
            "InsuranceName2": rng.choice(cfg.SUPPLEMENT_PLANS) if supplement else "",
            "On-board Date": f"2024-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}",
            "Member_Status": status,
            "Health Coach": rng.choice(cfg.COACHES),
            "Relationship_Status": rng.choice(
                ["Married", "Single", "Widowed", "Divorced"]
            ),
            "Preferred_Language": rng.choice(["English", "English", "Spanish"]),
            "DOB": dob.strftime("%m/%d/%Y"),
            "ID": sid,
        }
        return row

    def _height(self) -> str:
        inches = self.rng.randint(58, 76)
        return f"{inches // 12}'{inches % 12}\""

    def _contact_name(self) -> str:
        rel = self.rng.choice(cfg.EMERGENCY_RELATIONSHIPS)
        return f"{self.fake.first_name()} {self.fake.last_name()} ({rel})"

    # -- devices ----------------------------------------------------------

    def device(
        self, kind: str, sid: int, *, resupply: bool = False, n: int = 1
    ) -> Device:
        if kind == "bg":
            return Device(
                "Tenovi",
                f"SYN-BG-{sid:06d}-{n}",
                f"Tenovi Glucometer ({cfg.SYNTHETIC_MARKER})",
                resupply,
            )
        return Device(
            "Omron",
            f"SYN-BP-{sid:06d}-{n}",
            f"Omron Blood Pressure Cuff ({cfg.SYNTHETIC_MARKER})",
            resupply,
        )

    # -- readings ---------------------------------------------------------

    def reading_at(
        self, kind: str, recorded: datetime, latency_min: int | None = None
    ) -> Reading:
        rng = self.rng
        latency = rng.randint(1, 25) if latency_min is None else latency_min
        received = recorded + timedelta(minutes=latency)
        if kind == "bg":
            values: tuple[float | None, ...] = (float(rng.randint(72, 240)),)
        else:
            values = (float(rng.randint(105, 165)), float(rng.randint(62, 98)))
        return Reading(kind, recorded, received, values, manual=rng.random() < 0.1)

    def daily_readings(
        self, kind: str, days: list[date], per_day: int = 1
    ) -> list[Reading]:
        """Readings recorded and received on the same calendar day (6am-9pm)."""
        out = []
        for d in days:
            for _ in range(per_day):
                recorded = datetime(
                    d.year,
                    d.month,
                    d.day,
                    self.rng.randint(6, 21),
                    self.rng.randint(0, 59),
                )
                out.append(self.reading_at(kind, recorded))
        return out

    # -- notes ------------------------------------------------------------

    def note(
        self,
        when: datetime,
        seconds: int | None,
        *,
        note_type: str = "Follow-Up",
        upn: str | None = None,
        html: bool = False,
    ) -> Note:
        templates = cfg.NOTE_TEMPLATES_HTML if html else cfg.NOTE_TEMPLATES
        body = f"{cfg.SYNTHETIC_MARKER} note: {self.rng.choice(templates)}"
        return Note(
            timestamp=when,
            upn=upn or self.rng.choice(cfg.COACHES),
            body=body,
            note_type=note_type,
            seconds=seconds,
            auto_time=self.rng.random() < 0.7,
        )

    def note_on(self, d: date, seconds: int | None, **kw) -> Note:
        when = datetime(
            d.year, d.month, d.day, self.rng.randint(8, 17), self.rng.randint(0, 59)
        )
        return self.note(when, seconds, **kw)
