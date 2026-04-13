from dataclasses import dataclass
import pandas as pd
from app.services.constants import PARAMETERS
@dataclass
class AllocationRow:
    disease: str
    province: str
    stratum_code: str
    cascade_stage: str
    intervention_name: str
    unit_cost_zar: float
    daly_per_unit: float
    max_units: float
    source_name: str | None = None
    source_type: str | None = None
    source_tier: str | None = None
    daly_family: str = 'disease'
    daly_definition: str = ''
    country: str = 'South Africa'
    year: int | None = None
    is_sa_specific: bool = False
    is_fallback: bool = False
    is_imputed: bool = False
    uncertainty_low: float | None = None
    uncertainty_high: float | None = None
    notes: str | None = None
    parameter_provenance: dict | None = None

def rows_from_dataframe(df: pd.DataFrame):
    rows = []
    for _, r in df.iterrows():
        parameter_provenance = {}
        for param in PARAMETERS:
            parameter_provenance[param] = {
                'source_name': None if pd.isna(r.get(f'{param}_source_name')) else str(r.get(f'{param}_source_name')),
                'source_type': None if pd.isna(r.get(f'{param}_source_type')) else str(r.get(f'{param}_source_type')),
                'source_tier': None if pd.isna(r.get(f'{param}_source_tier')) else str(r.get(f'{param}_source_tier')),
                'source_year': None if pd.isna(r.get(f'{param}_source_year')) else int(r.get(f'{param}_source_year')),
                'retrieval_date': None if pd.isna(r.get(f'{param}_retrieval_date')) else str(r.get(f'{param}_retrieval_date')),
                'fill_type': None if pd.isna(r.get(f'{param}_fill_type')) else str(r.get(f'{param}_fill_type')),
                'confidence_flag': None if pd.isna(r.get(f'{param}_confidence_flag')) else str(r.get(f'{param}_confidence_flag')),
                'is_imputed': False if pd.isna(r.get(f'{param}_is_imputed', False)) else bool(r.get(f'{param}_is_imputed', False)),
            }
        rows.append(AllocationRow(
            disease=str(r['disease']), province=str(r['province']), stratum_code=str(r['stratum_code']),
            cascade_stage=str(r['cascade_stage']), intervention_name=str(r['intervention_name']),
            unit_cost_zar=float(r['unit_cost_zar']), daly_per_unit=float(r['daly_per_unit']),
            max_units=float(r['max_units']), source_name=None if pd.isna(r.get('source_name')) else str(r.get('source_name')),
            source_type=None if pd.isna(r.get('source_type')) else str(r.get('source_type')),
            source_tier=None if pd.isna(r.get('source_tier')) else str(r.get('source_tier')),
            daly_family=str(r.get('daly_family')), daly_definition=str(r.get('daly_definition')),
            country=str(r.get('country')), year=None if pd.isna(r.get('year')) else int(r.get('year')),
            is_sa_specific=False if pd.isna(r.get('is_sa_specific', False)) else bool(r.get('is_sa_specific', False)), is_fallback=False if pd.isna(r.get('is_fallback', False)) else bool(r.get('is_fallback', False)),
            is_imputed=False if pd.isna(r.get('is_imputed', False)) else bool(r.get('is_imputed', False)), uncertainty_low=None if pd.isna(r.get('uncertainty_low')) else float(r.get('uncertainty_low')),
            uncertainty_high=None if pd.isna(r.get('uncertainty_high')) else float(r.get('uncertainty_high')),
            notes=None if pd.isna(r.get('notes')) else str(r.get('notes')), parameter_provenance=parameter_provenance,
        ))
    return rows

def greedy_allocate(rows, total_budget, equity_weights=None, scenario_stage_weights=None):
    weighted_rows = []
    for row in rows:
        if row.unit_cost_zar <= 0:
            continue
        eq = equity_weights.get(row.stratum_code, 1.0) if equity_weights else 1.0
        sw = scenario_stage_weights.get(row.cascade_stage, 1.0) if scenario_stage_weights else 1.0
        score = (row.daly_per_unit / row.unit_cost_zar) * eq * sw
        weighted_rows.append((score, row, eq, sw))
    weighted_rows.sort(key=lambda x: x[0], reverse=True)
    allocations = []
    remaining_budget = total_budget
    for score, row, eq, sw in weighted_rows:
        if remaining_budget <= 0:
            break
        affordable_units = remaining_budget / row.unit_cost_zar
        units_allocated = min(row.max_units, affordable_units)
        if units_allocated <= 0:
            continue
        spend = units_allocated * row.unit_cost_zar
        dalys = units_allocated * row.daly_per_unit
        allocations.append({
            'disease': row.disease, 'province': row.province, 'stratum_code': row.stratum_code,
            'cascade_stage': row.cascade_stage, 'intervention_name': row.intervention_name,
            'units_allocated': round(units_allocated,4), 'spend_zar': round(spend,4), 'dalys_averted': round(dalys,4),
            'score': round(score,8), 'equity_weight': round(eq,4), 'stage_weight': round(sw,4),
            'source_name': row.source_name, 'source_type': row.source_type, 'source_tier': row.source_tier,
            'daly_family': row.daly_family, 'daly_definition': row.daly_definition, 'country': row.country, 'year': row.year,
            'is_sa_specific': row.is_sa_specific, 'is_fallback': row.is_fallback, 'is_imputed': row.is_imputed,
            'uncertainty_low': row.uncertainty_low, 'uncertainty_high': row.uncertainty_high, 'notes': row.notes,
            'parameter_provenance': row.parameter_provenance,
        })
        remaining_budget -= spend
    return allocations, round(remaining_budget, 4)

def summarise_kpis(allocations, budget_zar, budget_remaining):
    total_spend = sum(a['spend_zar'] for a in allocations)
    total_dalys = sum(a['dalys_averted'] for a in allocations)
    total_units = sum(a['units_allocated'] for a in allocations)
    avg = (total_spend / total_dalys) if total_dalys > 0 else None
    return {
        'budget_zar': round(budget_zar,4), 'total_spend_zar': round(total_spend,4), 'budget_remaining_zar': round(budget_remaining,4),
        'total_dalys_averted': round(total_dalys,4), 'total_units_allocated': round(total_units,4),
        'average_cost_per_daly': round(avg,4) if avg is not None else None,
    }
