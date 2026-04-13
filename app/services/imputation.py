from datetime import datetime, timezone
import pandas as pd
IMPUTABLE_COLUMNS = ['prevalence', 'baseline_coverage', 'max_coverage', 'unit_cost_zar', 'daly_per_unit']
UTC_NOW = lambda: datetime.now(timezone.utc).isoformat()

def simple_impute(df: pd.DataFrame):
    work = df.copy()
    summary = {}
    original_missing = {col: work[col].isna() for col in IMPUTABLE_COLUMNS if col in work.columns}
    for col in IMPUTABLE_COLUMNS:
        if col in work.columns:
            missing_before = int(work[col].isna().sum())
            if 'disease' in work.columns and 'cascade_stage' in work.columns:
                work[col] = work.groupby(['disease','cascade_stage'])[col].transform(lambda s: s.fillna(s.median()))
            if 'province' in work.columns:
                work[col] = work.groupby(['province'])[col].transform(lambda s: s.fillna(s.median()))
            work[col] = work[col].fillna(work[col].median())
            summary[f'{col}_imputed'] = missing_before
    if {'baseline_coverage','max_coverage'}.issubset(work.columns):
        work['baseline_coverage'] = work['baseline_coverage'].clip(lower=0, upper=1)
        work['max_coverage'] = work['max_coverage'].clip(lower=0, upper=1)
        work.loc[work['max_coverage'] < work['baseline_coverage'], 'max_coverage'] = work['baseline_coverage']
    for col in IMPUTABLE_COLUMNS:
        for suffix in ['is_imputed','source_name','source_type','source_tier','source_year','retrieval_date','fill_type','confidence_flag']:
            name = f'{col}_{suffix}'
            if name not in work.columns:
                work[name] = pd.NA
    for col, mask in original_missing.items():
        work.loc[mask, f'{col}_is_imputed'] = True
        work.loc[mask & work[f'{col}_source_name'].isna(), f'{col}_source_name'] = 'Grouped median imputation'
        work.loc[mask & work[f'{col}_source_type'].isna(), f'{col}_source_type'] = 'imputation'
        work.loc[mask & work[f'{col}_source_tier'].isna(), f'{col}_source_tier'] = 'imputed'
        work.loc[mask & work[f'{col}_retrieval_date'].isna(), f'{col}_retrieval_date'] = UTC_NOW()
        work.loc[mask & work[f'{col}_fill_type'].isna(), f'{col}_fill_type'] = 'imputed'
        work.loc[mask & work[f'{col}_confidence_flag'].isna(), f'{col}_confidence_flag'] = 'low'
    any_imputed = pd.Series(False, index=work.index)
    for col in IMPUTABLE_COLUMNS:
        any_imputed = any_imputed | work[f'{col}_is_imputed'].fillna(False).astype(bool)
    if 'is_imputed' not in work.columns:
        work['is_imputed'] = False
    work.loc[any_imputed, 'is_imputed'] = True
    if 'notes' not in work.columns:
        work['notes'] = None
    work.loc[any_imputed & work['notes'].isna(), 'notes'] = 'One or more parameters were filled using grouped median rules.'
    return work, summary
