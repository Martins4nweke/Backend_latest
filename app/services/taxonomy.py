import re

CANONICAL_DISEASE_ALIASES = {
    "hypertension": "Hypertension",
    "high blood pressure": "Hypertension",
    "raised blood pressure": "Hypertension",
    "bp": "Hypertension",
    "diabetes": "Diabetes",
    "diabetes mellitus": "Diabetes",
    "type 2 diabetes": "Diabetes",
    "type 1 diabetes": "Diabetes",
    "cervical cancer": "Cervical cancer",
    "ca cervix": "Cervical cancer",
    "breast cancer": "Breast cancer",
}

INTERVENTION_ALIASES = {
    "bp screening": "BP screening",
    "blood pressure screening": "BP screening",
    "confirmatory bp measurement": "Confirmatory BP measurement",
    "confirmatory blood pressure measurement": "Confirmatory BP measurement",
    "antihypertensive initiation": "Antihypertensive initiation",
    "follow-up visit": "Follow-up visit",
    "follow up visit": "Follow-up visit",
    "adherence counselling": "Adherence counselling",
    "blood glucose screening": "Blood glucose screening",
    "hba1c confirmation": "HbA1c confirmation",
    "oral treatment initiation": "Oral treatment initiation",
    "insulin initiation": "Insulin initiation",
    "glucose monitoring visit": "Glucose monitoring visit",
    "lifestyle and medication adherence support": "Lifestyle and medication adherence support",
}

WHO_CHOICE_BUNDLES = {
    "BP screening": "outpatient_primary",
    "Confirmatory BP measurement": "outpatient_primary",
    "Antihypertensive initiation": "outpatient_primary",
    "Follow-up visit": "outpatient_primary",
    "Adherence counselling": "outpatient_primary",
    "Blood glucose screening": "outpatient_primary",
    "HbA1c confirmation": "outpatient_secondary",
    "Oral treatment initiation": "outpatient_primary",
    "Insulin initiation": "outpatient_secondary",
    "Glucose monitoring visit": "outpatient_primary",
}

GBD_CAUSE_ALIASES = {
    "Hypertension": [
        "High systolic blood pressure",
        "high systolic blood pressure",
        "raised blood pressure",
        "hypertension",
    ],
    "Diabetes": [
        "Diabetes mellitus",
        "diabetes mellitus",
        "diabetes",
    ],
    "Cervical cancer": ["Cervical cancer"],
    "Breast cancer": ["Breast cancer"],
}

WHO_GHO_INDICATOR_HINTS = {
    "Hypertension": [
        "Raised blood pressure",
        "blood pressure",
        "hypertension",
    ],
    "Diabetes": [
        "Diabetes mellitus",
        "diabetes",
        "raised fasting blood glucose",
    ],
    "Cervical cancer": [
        "cervical cancer",
        "cervix",
    ],
    "Breast cancer": [
        "breast cancer",
        "breast",
    ],
}

COUNTRY_CODE_MAP = {
    "south africa": "ZA",
    "nigeria": "NG",
    "ghana": "GH",
    "kenya": "KE",
    "uganda": "UG",
    "tanzania": "TZ",
    "zambia": "ZM",
    "botswana": "BW",
    "namibia": "NA",
    "united states": "US",
    "united kingdom": "GB",
    "india": "IN",
    "ethiopia": "ET",
    "rwanda": "RW",
    "malawi": "MW",
    "zimbabwe": "ZW",
    "cameroon": "CM",
    "senegal": "SN",
}

def _norm(text: str | None) -> str:
    if text is None:
        return ""
    text = str(text).strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text

def canonical_disease(name: str | None) -> str:
    cleaned = _norm(name)
    return CANONICAL_DISEASE_ALIASES.get(cleaned, str(name).strip() if name is not None else "")

def canonical_intervention(name: str | None) -> str:
    cleaned = _norm(name)
    return INTERVENTION_ALIASES.get(cleaned, str(name).strip() if name is not None else "")

def canonical_country(name: str | None) -> str:
    if name is None:
        return ""
    return str(name).strip()

def country_code(name: str | None) -> str | None:
    return COUNTRY_CODE_MAP.get(_norm(name))

def gbd_search_terms(disease: str | None) -> list[str]:
    canon = canonical_disease(disease)
    return GBD_CAUSE_ALIASES.get(canon, [canon])

def who_choice_bundle(intervention: str | None) -> str | None:
    canon = canonical_intervention(intervention)
    return WHO_CHOICE_BUNDLES.get(canon)

def who_gho_indicator_hints(disease: str | None) -> list[str]:
    canon = canonical_disease(disease)
    return WHO_GHO_INDICATOR_HINTS.get(canon, [canon])
