import pandas as pd
from app.services.constants import REQUIRED_COLUMNS, REQUIRED_METADATA_COLUMNS, UNIVERSAL_CASCADE
from app.services.disease_registry import get_module

def validate_input_dataframe(df: pd.DataFrame):
    errors, warnings = [], []
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")
        return errors, warnings
    meta_missing = [c for c in REQUIRED_METADATA_COLUMNS if c not in df.columns]
    if meta_missing:
        warnings.append(f"Missing recommended metadata columns: {', '.join(meta_missing)}")
    for col in ["population", "prevalence", "baseline_coverage", "max_coverage", "unit_cost_zar", "daly_per_unit"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[col].isna().all():
                errors.append(f"{col} could not be interpreted as numeric")
    if 'baseline_coverage' in df.columns and ((df['baseline_coverage'] < 0) | (df['baseline_coverage'] > 1)).any():
        errors.append('baseline_coverage must be between 0 and 1')
    if 'max_coverage' in df.columns and ((df['max_coverage'] < 0) | (df['max_coverage'] > 1)).any():
        errors.append('max_coverage must be between 0 and 1')
    if {'baseline_coverage','max_coverage'}.issubset(df.columns) and (df['max_coverage'] < df['baseline_coverage']).any():
        errors.append('max_coverage cannot be lower than baseline_coverage')
    if 'population' in df.columns and (df['population'] <= 0).any():
        errors.append('population must be greater than 0')
    if 'unit_cost_zar' in df.columns and (df['unit_cost_zar'] <= 0).any():
        errors.append('unit_cost_zar must be greater than 0')
    if 'prevalence' in df.columns and (df['prevalence'] < 0).any():
        errors.append('prevalence cannot be negative')
    if 'cascade_stage' in df.columns:
        invalid = sorted(set(df['cascade_stage'].dropna()) - set(UNIVERSAL_CASCADE))
        if invalid:
            errors.append(f"Invalid cascade_stage values: {', '.join(map(str, invalid))}")
    if {'disease','cascade_stage','intervention_name'}.issubset(df.columns):
        for idx, row in df[['disease','cascade_stage','intervention_name']].dropna().iterrows():
            module = get_module(str(row['disease']))
            if module is None:
                errors.append(f"Unknown disease in row {idx + 2}: {row['disease']}")
                continue
            allowed = module['interventions'].get(str(row['cascade_stage']), [])
            if str(row['intervention_name']) not in allowed:
                errors.append(f"Invalid intervention '{row['intervention_name']}' for disease '{row['disease']}' and stage '{row['cascade_stage']}' in row {idx + 2}")
    if {'daly_family','daly_definition'}.issubset(df.columns):
        mixed = set(df['daly_family'].dropna().astype(str).str.lower().unique())
        if 'disease' in mixed and 'risk_factor' in mixed:
            warnings.append('Disease DALYs and risk-factor DALYs are both present. Ensure the mixture is explicitly justified.')
    return errors, warnings

def summarise_missingness(df: pd.DataFrame):
    return {c: int(df[c].isna().sum()) for c in df.columns if int(df[c].isna().sum()) > 0}

def summarise_provenance(df: pd.DataFrame):
    out = {
        'rows_total': int(len(df)),
        'rows_with_source_name': int(df['source_name'].notna().sum()) if 'source_name' in df.columns else 0,
        'rows_with_source_type': int(df['source_type'].notna().sum()) if 'source_type' in df.columns else 0,
        'rows_with_source_tier': int(df['source_tier'].notna().sum()) if 'source_tier' in df.columns else 0,
        'rows_marked_imputed': int(df['is_imputed'].sum()) if 'is_imputed' in df.columns else 0,
    }
    if 'source_tier' in df.columns:
        out['source_tier_breakdown'] = df['source_tier'].fillna('missing').value_counts(dropna=False).to_dict()
    return out
