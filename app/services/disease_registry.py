DISEASE_MODULES = {
    "Hypertension": {
        "disease": "Hypertension",
        "country_default": "South Africa",
        "cascade_stages": ["screening", "diagnosis", "treatment_initiation", "follow_up", "adherence"],
        "interventions": {
            "screening": ["BP screening"],
            "diagnosis": ["Confirmatory BP measurement"],
            "treatment_initiation": ["Antihypertensive initiation"],
            "follow_up": ["Follow-up visit"],
            "adherence": ["Adherence counselling"],
        },
        "daly_family_default": "risk_factor",
        "daly_definition_default": "DALYs attributable to high systolic blood pressure",
        "preferred_source_order": ["sa_user_or_observed", "sa_modelled", "gbd_sa", "who_choice_sa_or_regional", "contextualized_global", "imputed"],
        "notes": "Use risk-factor DALYs unless a disease-specific hypertension burden is explicitly justified.",
    },
    "Diabetes": {
        "disease": "Diabetes",
        "country_default": "South Africa",
        "cascade_stages": ["screening", "diagnosis", "treatment_initiation", "follow_up", "adherence"],
        "interventions": {
            "screening": ["Blood glucose screening"],
            "diagnosis": ["HbA1c confirmation"],
            "treatment_initiation": ["Oral treatment initiation", "Insulin initiation"],
            "follow_up": ["Glucose monitoring visit"],
            "adherence": ["Lifestyle and medication adherence support"],
        },
        "daly_family_default": "disease",
        "daly_definition_default": "DALYs due to diabetes mellitus",
        "preferred_source_order": ["sa_user_or_observed", "sa_modelled", "gbd_sa", "who_choice_sa_or_regional", "contextualized_global", "imputed"],
        "notes": "Use disease DALYs unless risk-attributable modelling is explicitly selected and documented.",
    },
    "Cervical cancer": {
        "disease": "Cervical cancer",
        "country_default": "South Africa",
        "cascade_stages": ["screening", "diagnosis", "treatment_initiation", "follow_up", "adherence"],
        "interventions": {
            "screening": ["HPV testing", "Pap smear"],
            "diagnosis": ["Colposcopy and biopsy"],
            "treatment_initiation": ["Pre-cancer treatment", "Cancer treatment initiation"],
            "follow_up": ["Post-treatment follow up"],
            "adherence": ["Retention and treatment completion support"],
        },
        "daly_family_default": "disease",
        "daly_definition_default": "DALYs due to cervical cancer",
        "preferred_source_order": ["sa_user_or_observed", "sa_modelled", "gbd_sa", "who_choice_sa_or_regional", "contextualized_global", "imputed"],
        "notes": "Use disease DALYs and avoid substituting risk-factor DALYs unless explicitly justified.",
    },
    "Breast cancer": {
        "disease": "Breast cancer",
        "country_default": "South Africa",
        "cascade_stages": ["screening", "diagnosis", "treatment_initiation", "follow_up", "adherence"],
        "interventions": {
            "screening": ["Clinical breast screening", "Mammography"],
            "diagnosis": ["Imaging and biopsy confirmation"],
            "treatment_initiation": ["Cancer treatment initiation"],
            "follow_up": ["Post-treatment follow up"],
            "adherence": ["Treatment continuation support"],
        },
        "daly_family_default": "disease",
        "daly_definition_default": "DALYs due to breast cancer",
        "preferred_source_order": ["sa_user_or_observed", "sa_modelled", "gbd_sa", "who_choice_sa_or_regional", "contextualized_global", "imputed"],
        "notes": "Use disease-specific DALYs and clearly document source year and uncertainty.",
    },
}

def list_modules():
    return list(DISEASE_MODULES.values())

def get_module(disease: str):
    return DISEASE_MODULES.get(disease)
