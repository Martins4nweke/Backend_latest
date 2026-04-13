import pandas as pd
from app.services.disease_registry import get_module
from app.services.constants import STAGE_MAX_DEFAULT, PARAMETERS
DEFAULT_SOURCE_FIELDS = {
    'source_name': None,
    'source_type': None,
    'source_tier': None,
    'is_sa_specific': False,
    'is_fallback': False,
    'is_imputed': False,
    'notes': None,
}

def _append_note(existing, addition):
    if pd.isna(existing) or str(existing).strip() == '':
        return addition
    if addition in str(existing):
        return str(existing)
    return f"{existing}; {addition}"

def _fill_metadata_defaults(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for col, default in DEFAULT_SOURCE_FIELDS.items():
        if col not in work.columns:
            work[col] = default
    if 'country' not in work.columns:
        work['country'] = 'South Africa'
    if 'year' not in work.columns:
        work['year'] = 2021
    if 'uncertainty_low' not in work.columns:
        work['uncertainty_low'] = pd.NA
    if 'uncertainty_high' not in work.columns:
        work['uncertainty_high'] = pd.NA
    for param in PARAMETERS:
        for suffix in ['source_name','source_type','source_tier','is_imputed','source_year','retrieval_date','fill_type','confidence_flag']:
            col = f"{param}_{suffix}"
            if col not in work.columns:
                work[col] = pd.NA
    return work

def _refresh_row_level_provenance(work: pd.DataFrame) -> pd.DataFrame:
    for idx, row in work.iterrows():
        any_imputed = False
        tiers, names, types = [], [], []
        for param in PARAMETERS:
            if pd.notna(row.get(f"{param}_is_imputed")) and bool(row.get(f"{param}_is_imputed")):
                any_imputed = True
            tier = row.get(f"{param}_source_tier")
            name = row.get(f"{param}_source_name")
            typ = row.get(f"{param}_source_type")
            if pd.notna(tier): tiers.append(str(tier))
            if pd.notna(name): names.append(str(name))
            if pd.notna(typ): types.append(str(typ))
        work.at[idx, 'is_imputed'] = any_imputed
        if any_imputed:
            work.at[idx, 'source_tier'] = 'mixed' if len(set(tiers)) > 1 else (tiers[0] if tiers else 'imputed')
            work.at[idx, 'source_name'] = 'Multiple parameter sources' if len(set(names)) > 1 else (names[0] if names else 'Assembled row')
            work.at[idx, 'source_type'] = 'mixed' if len(set(types)) > 1 else (types[0] if types else 'assembled')
        else:
            if tiers: work.at[idx, 'source_tier'] = tiers[0]
            if names: work.at[idx, 'source_name'] = names[0]
            if types: work.at[idx, 'source_type'] = types[0]
    return work

def assemble_minimum_dataset(df: pd.DataFrame):
    work = _fill_metadata_defaults(df)
    summary = {'module_defaults_applied':0,'metadata_defaults_added':0,'rule_based_max_coverage_defaults_applied':0,'fallback_flags_added':0,'rows_marked_imputed_during_assembly':0}
    for idx, row in work.iterrows():
        module = get_module(str(row['disease'])) if pd.notna(row.get('disease')) else None
        if module is None:
            continue
        if pd.isna(row.get('daly_family')) or str(row.get('daly_family')).strip() == '':
            work.at[idx, 'daly_family'] = module['daly_family_default']
            summary['module_defaults_applied'] += 1
        if pd.isna(row.get('daly_definition')) or str(row.get('daly_definition')).strip() == '':
            work.at[idx, 'daly_definition'] = module['daly_definition_default']
            summary['module_defaults_applied'] += 1
        if pd.isna(row.get('country')) or str(row.get('country')).strip() == '':
            work.at[idx, 'country'] = module['country_default']
            summary['metadata_defaults_added'] += 1
        if pd.isna(row.get('source_tier')) or str(row.get('source_tier')).strip() == '':
            work.at[idx, 'source_tier'] = 'country_official' if str(work.at[idx, 'country']) == 'South Africa' else 'global_official'
            summary['metadata_defaults_added'] += 1
        if pd.isna(row.get('source_type')) or str(row.get('source_type')).strip() == '':
            work.at[idx, 'source_type'] = 'user_uploaded'
        if pd.isna(row.get('source_name')) or str(row.get('source_name')).strip() == '':
            work.at[idx, 'source_name'] = 'User uploaded dataset'
        if pd.isna(row.get('is_fallback')):
            work.at[idx, 'is_fallback'] = False
        for param in PARAMETERS:
            if pd.notna(row.get(param)):
                if pd.isna(row.get(f"{param}_source_name")):
                    work.at[idx, f"{param}_source_name"] = work.at[idx, 'source_name']
                if pd.isna(row.get(f"{param}_source_type")):
                    work.at[idx, f"{param}_source_type"] = work.at[idx, 'source_type']
                if pd.isna(row.get(f"{param}_source_tier")):
                    work.at[idx, f"{param}_source_tier"] = 'uploaded'
                if pd.isna(row.get(f"{param}_is_imputed")):
                    work.at[idx, f"{param}_is_imputed"] = False
                if pd.isna(row.get(f"{param}_fill_type")):
                    work.at[idx, f"{param}_fill_type"] = 'uploaded'
                if pd.isna(row.get(f"{param}_confidence_flag")):
                    work.at[idx, f"{param}_confidence_flag"] = 'high'
        stage = str(row.get('cascade_stage')) if pd.notna(row.get('cascade_stage')) else None
        current_max = row.get('max_coverage')
        if (pd.isna(current_max) or str(current_max).strip() == '') and stage in STAGE_MAX_DEFAULT:
            default_max = STAGE_MAX_DEFAULT[stage]
            baseline = row.get('baseline_coverage')
            if pd.notna(baseline):
                default_max = max(default_max, float(baseline))
            work.at[idx, 'max_coverage'] = default_max
            work.at[idx, 'max_coverage_source_name'] = 'Stage-based default'
            work.at[idx, 'max_coverage_source_type'] = 'rule_based_imputation'
            work.at[idx, 'max_coverage_source_tier'] = 'imputed'
            work.at[idx, 'max_coverage_is_imputed'] = True
            work.at[idx, 'max_coverage_fill_type'] = 'imputed'
            work.at[idx, 'max_coverage_confidence_flag'] = 'low'
            work.at[idx, 'is_fallback'] = True
            work.at[idx, 'notes'] = _append_note(work.at[idx, 'notes'], f"Stage-based max_coverage default applied for {stage}.")
            summary['rule_based_max_coverage_defaults_applied'] += 1
            summary['rows_marked_imputed_during_assembly'] += 1
            if pd.isna(work.at[idx, 'uncertainty_low']):
                work.at[idx, 'uncertainty_low'] = max(0.0, round(float(default_max) - 0.10, 4))
            if pd.isna(work.at[idx, 'uncertainty_high']):
                work.at[idx, 'uncertainty_high'] = min(1.0, round(float(default_max) + 0.05, 4))
        if bool(work.at[idx, 'is_fallback']):
            summary['fallback_flags_added'] += 1
    work = _refresh_row_level_provenance(work)
    return work, summary
