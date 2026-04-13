from app.services.constants import DISADVANTAGED_STRATA

def compute_equity_metrics(rows):
    total_dalys = sum(float(r.get('dalys_averted', 0)) for r in rows)
    total_spend = sum(float(r.get('spend_zar', 0)) for r in rows)
    disadv_rows = [r for r in rows if r.get('stratum_code') in DISADVANTAGED_STRATA]
    other_rows = [r for r in rows if r.get('stratum_code') not in DISADVANTAGED_STRATA]
    disadv_dalys = sum(float(r.get('dalys_averted', 0)) for r in disadv_rows)
    disadv_spend = sum(float(r.get('spend_zar', 0)) for r in disadv_rows)
    other_dalys = sum(float(r.get('dalys_averted', 0)) for r in other_rows)
    other_spend = sum(float(r.get('spend_zar', 0)) for r in other_rows)
    return {
        'disadvantaged_dalys': round(disadv_dalys, 4),
        'disadvantaged_spend_zar': round(disadv_spend, 4),
        'disadvantaged_daly_share': round((disadv_dalys / total_dalys), 4) if total_dalys > 0 else 0,
        'disadvantaged_spend_share': round((disadv_spend / total_spend), 4) if total_spend > 0 else 0,
        'disadvantaged_vs_other_spend_ratio': round((disadv_spend / other_spend), 4) if other_spend > 0 else None,
        'disadvantaged_vs_other_daly_ratio': round((disadv_dalys / other_dalys), 4) if other_dalys > 0 else None,
        'equity_gap_flag': bool((disadv_spend / total_spend) < 0.5) if total_spend > 0 else False,
    }

def compute_budget_diagnostics(kpis, grouped, rows):
    budget = float(kpis.get('budget_zar', 0) or 0)
    spend = float(kpis.get('total_spend_zar', 0) or 0)
    remaining = float(kpis.get('budget_remaining_zar', 0) or 0)
    util = (spend / budget) if budget > 0 else 0
    reason = 'budget_exhausted'
    flag = False
    if remaining > 0:
        flag = True
        if util < 0.5:
            reason = 'material_underutilisation_likely_due_to_scale_limits_or_conservative_caps'
        elif util < 0.8:
            reason = 'moderate_underutilisation_likely_due_to_headroom_or_eligible_pool_constraints'
        else:
            reason = 'minor_underutilisation'
    return {
        'budget_utilisation_rate': round(util, 4),
        'budget_underuse_flag': flag,
        'budget_underuse_reason': reason,
        'max_reachable_spend_proxy': round(spend, 4),
        'headroom_constraint_summary': 'Allocation is capped by eligible units and coverage ceilings rather than budget alone.',
    }

def compute_structure_diagnostics(grouped):
    by_intervention = grouped.get('by_intervention', [])
    by_stage = grouped.get('by_stage', [])
    by_province = grouped.get('by_province', [])
    by_stratum = grouped.get('by_stratum', [])
    top_intervention = by_intervention[0] if by_intervention else None
    weakest_stage = by_stage[-1] if by_stage else None
    top_stage = by_stage[0] if by_stage else None
    top_province = by_province[0] if by_province else None
    top_stratum = by_stratum[0] if by_stratum else None
    return {
        'top_intervention_name': top_intervention['name'] if top_intervention else None,
        'top_intervention_spend_share': top_intervention['spend_share'] if top_intervention else None,
        'top_stage_name': top_stage['name'] if top_stage else None,
        'top_stage_spend_share': top_stage['spend_share'] if top_stage else None,
        'weakest_stage_by_spend': weakest_stage['name'] if weakest_stage else None,
        'weakest_stage_by_dalys': sorted(by_stage, key=lambda x: x.get('dalys', 0))[0]['name'] if by_stage else None,
        'top_province_name': top_province['name'] if top_province else None,
        'top_stratum_name': top_stratum['name'] if top_stratum else None,
        'cascade_bottleneck_flag': bool(top_stage and weakest_stage and top_stage['name'] != weakest_stage['name']),
        'cascade_balance_summary': 'balanced' if len(by_stage) >= 2 and abs(by_stage[0]['spend_share'] - by_stage[-1]['spend_share']) < 0.15 else 'skewed',
    }

def compute_parameter_provenance_summary(rows):
    summary = {
        'rows_with_imputed_max_coverage': 0,
        'rows_with_imputed_daly': 0,
        'rows_with_imputed_cost': 0,
        'rows_with_imputed_prevalence': 0,
        'rows_with_imputed_baseline_coverage': 0,
        'rows_with_public_population_fill': 0,
        'rows_with_public_prevalence_fill': 0,
        'rows_with_public_cost_fill': 0,
        'rows_with_public_daly_fill': 0,
    }
    for row in rows:
        prov = row.get('parameter_provenance', {})
        if prov.get('max_coverage', {}).get('is_imputed'):
            summary['rows_with_imputed_max_coverage'] += 1
        if prov.get('daly_per_unit', {}).get('is_imputed'):
            summary['rows_with_imputed_daly'] += 1
        if prov.get('unit_cost_zar', {}).get('is_imputed'):
            summary['rows_with_imputed_cost'] += 1
        if prov.get('prevalence', {}).get('is_imputed'):
            summary['rows_with_imputed_prevalence'] += 1
        if prov.get('baseline_coverage', {}).get('is_imputed'):
            summary['rows_with_imputed_baseline_coverage'] += 1
        if prov.get('population', {}).get('source_tier') in {'country_official', 'global_official'}:
            summary['rows_with_public_population_fill'] += 1
        if prov.get('prevalence', {}).get('source_tier') in {'country_official', 'global_official', 'country_study'}:
            summary['rows_with_public_prevalence_fill'] += 1
        if prov.get('unit_cost_zar', {}).get('source_tier') in {'country_official', 'global_costing', 'country_study'}:
            summary['rows_with_public_cost_fill'] += 1
        if prov.get('daly_per_unit', {}).get('source_tier') in {'global_burden', 'country_study'}:
            summary['rows_with_public_daly_fill'] += 1
    return summary

def build_budget_diagnostics_table(kpis, budget_diag):
    return [
        {'metric': 'Budget', 'value': kpis.get('budget_zar')},
        {'metric': 'Spend', 'value': kpis.get('total_spend_zar')},
        {'metric': 'Budget remaining', 'value': kpis.get('budget_remaining_zar')},
        {'metric': 'Budget utilisation rate', 'value': budget_diag.get('budget_utilisation_rate')},
        {'metric': 'Underuse reason', 'value': budget_diag.get('budget_underuse_reason')},
    ]
