import pandas as pd
NUMERIC_COLUMNS = ["population", "prevalence", "baseline_coverage", "max_coverage", "unit_cost_zar", "daly_per_unit", "uncertainty_low", "uncertainty_high", "year"]
TEXT_COLUMNS = ["disease", "province", "stratum_code", "cascade_stage", "intervention_name", "daly_family", "daly_definition", "country", "source_name", "source_type", "source_tier", "notes", "population_basis"]
BOOL_COLUMNS = ["is_sa_specific", "is_fallback", "is_imputed"]

def standardise_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work.columns = [str(c).strip().lower() for c in work.columns]
    rename_map = {"intervention": "intervention_name", "stage": "cascade_stage", "cost": "unit_cost_zar", "daly": "daly_per_unit"}
    work = work.rename(columns={k: v for k, v in rename_map.items() if k in work.columns})
    for col in NUMERIC_COLUMNS:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors='coerce')
    for col in TEXT_COLUMNS:
        if col in work.columns:
            work[col] = work[col].astype('string').str.strip()
    for col in BOOL_COLUMNS:
        if col in work.columns:
            work[col] = work[col].astype(str).str.lower().isin(['1','true','yes'])
    return work

def apply_population_basis_rule(df: pd.DataFrame):
    work = df.copy()
    summary = {
        "population_basis_detected": "unknown",
        "effective_prevalence_rule_applied": "none",
        "prevalence_double_count_warning": False,
        "population_basis_notes": [],
    }
    if 'population_basis' in work.columns and work['population_basis'].notna().any():
        vals = set(work['population_basis'].dropna().astype(str).str.lower().unique())
        if vals == {'eligible_population'}:
            summary['population_basis_detected'] = 'eligible_population'
            if 'prevalence' in work.columns and ((work['prevalence'].fillna(1.0) != 1.0)).any():
                summary['prevalence_double_count_warning'] = True
                summary['population_basis_notes'].append('Eligible population was supplied together with prevalence values different from 1.0. Prevalence was forced to 1.0 to prevent double application.')
            work['prevalence'] = 1.0
            summary['effective_prevalence_rule_applied'] = 'forced_to_1.0'
        elif vals == {'general_population'}:
            summary['population_basis_detected'] = 'general_population'
        else:
            summary['population_basis_detected'] = 'mixed'
            summary['population_basis_notes'].append('Mixed population_basis values were detected across rows. Review the uploaded file carefully.')
    else:
        if 'prevalence' in work.columns and work['prevalence'].dropna().nunique() == 1 and float(work['prevalence'].dropna().iloc[0]) == 1.0:
            summary['population_basis_detected'] = 'eligible_population_assumed'
            summary['effective_prevalence_rule_applied'] = 'already_1.0'
    if 'prevalence' in work.columns and (work['prevalence'].dropna() > 1).any():
        summary['prevalence_double_count_warning'] = True
        summary['population_basis_notes'].append('Some prevalence values exceed 1.0. Review scale and confirm the prevalence column is not a percentage encoded as a whole number.')
    return work, summary

def prepare_allocation_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work['headroom'] = (work['max_coverage'] - work['baseline_coverage']).clip(lower=0)
    work['max_units'] = (work['population'] * work['prevalence'] * work['headroom']).clip(lower=0)
    return work
