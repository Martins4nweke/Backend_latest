UNIVERSAL_CASCADE = ["screening", "diagnosis", "treatment_initiation", "follow_up", "adherence"]
REQUIRED_COLUMNS = [
    "disease", "province", "stratum_code", "cascade_stage", "intervention_name",
    "population", "prevalence", "baseline_coverage", "max_coverage", "unit_cost_zar", "daly_per_unit",
]
REQUIRED_METADATA_COLUMNS = ["daly_family", "daly_definition", "country", "year"]
STAGE_MAX_DEFAULT = {
    "screening": 0.90,
    "diagnosis": 0.80,
    "treatment_initiation": 0.80,
    "follow_up": 0.75,
    "adherence": 0.70,
}
PARAMETERS = ["population", "prevalence", "baseline_coverage", "max_coverage", "unit_cost_zar", "daly_per_unit"]
DISADVANTAGED_STRATA = {"S1", "S2"}
