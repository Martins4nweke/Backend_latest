from app.services.diagnostics import compute_equity_metrics


def _province_bottleneck(grouped):
    stage = grouped.get('by_stage', [])
    return stage[-1]['name'] if stage else None


def build_province_comparative_table(grouped, disease):
    rows = []
    for item in grouped.get('by_province', []):
        rows.append({
            'province': item['name'],
            'spend_zar': item['spend'],
            'dalys_averted': item['dalys'],
            'cost_per_daly': item['cost_per_daly'],
            'dominant_intervention': grouped.get('by_intervention', [{}])[0].get('name') if grouped.get('by_intervention') else None,
            'bottleneck_stage': _province_bottleneck(grouped),
            'underuse_reason': 'headroom or eligible population constraint',
            'equity_concern': 'disadvantaged strata may still need explicit protection'
        })
    return rows


def generate_equity_recommendations(rows, grouped):
    recs = []
    eq = compute_equity_metrics(rows)
    if eq['equity_gap_flag']:
        recs.append('Increase the minimum allocation to disadvantaged strata because the current portfolio sends less than half of total spend to those groups.')
    else:
        recs.append('Maintain the current emphasis on disadvantaged strata while testing whether a moderate equity weight can improve coverage without major efficiency loss.')
    top = grouped['by_intervention'][0]['name'] if grouped.get('by_intervention') else None
    if top:
        recs.append(f'Pair any equity-focused reallocation with operational strengthening of {top}, since it currently provides the strongest return in the portfolio.')
    recs.append('Programme teams should review diagnosis, initiation, and follow-up bottlenecks so efficiency gains do not widen inequities across provinces or strata.')
    return recs


def generate_policy_bundle(kpis, grouped, rows, disease, budget_diag, structure_diag, equity_diag):
    if not rows:
        return {
            'policy_advisory_brief': 'No allocation results are available to generate policy guidance.',
            'policy_operational_brief': 'No operational interpretation is available.',
            'policy_equity_brief': 'No equity interpretation is available.',
            'policy_budget_brief': 'No budget interpretation is available.',
            'policy_scenario_brief': 'Run scenario comparison to generate scenario guidance.',
            'executive_summary': 'No reportable results were generated.'
        }

    top_intervention = structure_diag.get('top_intervention_name') or 'the leading intervention'
    top_stage = structure_diag.get('top_stage_name') or 'the leading stage'
    weakest_stage = structure_diag.get('weakest_stage_by_spend') or 'the weakest stage'
    top_province = structure_diag.get('top_province_name') or 'the highest-priority province'
    top_stratum = structure_diag.get('top_stratum_name') or 'the highest-priority stratum'

    national = (
        f'Advisory note for national and provincial decision-makers. This allocation run spent ZAR {kpis["total_spend_zar"]:,.0f} '
        f'and is projected to avert {kpis["total_dalys_averted"]:,.1f} DALYs at an average cost of '
        f'ZAR {kpis["average_cost_per_daly"]:,.0f} per DALY. The portfolio is driven primarily by '
        f'{top_intervention}, while {top_province} receives the largest provincial allocation and '
        f'{top_stratum} is the most funded stratum.'
    )

    operational = (
        f'Operational interpretation. The allocation is concentrated in {top_stage}, while {weakest_stage} '
        f'appears comparatively underfunded. Implementation teams should test whether better balance '
        f'across screening, diagnosis, treatment initiation, follow up, and adherence would improve '
        f'continuity of care.'
    )

    equity_text = (
        'This suggests a material equity shortfall that should be tested against an equity-weighted scenario.'
        if equity_diag['equity_gap_flag']
        else 'This suggests the current efficiency-led portfolio is still providing meaningful benefit to disadvantaged groups.'
    )
    equity = (
        f'Equity interpretation. Disadvantaged strata currently receive '
        f'{equity_diag["disadvantaged_spend_share"]*100:.1f}% of total spend and '
        f'{equity_diag["disadvantaged_daly_share"]*100:.1f}% of total DALYs. '
        f'{equity_text}'
    )

    budget_text = (
        'The remaining budget likely reflects binding scale limits such as exhausted coverage ceilings or eligible population constraints.'
        if budget_diag['budget_underuse_flag']
        else 'The budget was fully absorbed within the current feasible allocation envelope.'
    )
    budget = (
        f'Budget use interpretation. The model used {budget_diag["budget_utilisation_rate"]*100:.1f}% '
        f'of the available budget. {budget_text}'
    )

    scenario = 'Scenario recommendation. Compare the baseline allocation against an equity-sensitive scenario, a balanced-cascade scenario, and an efficiency-maximising scenario before final adoption.'
    executive = (
        f'Executive summary. For {disease}, the model favours {top_intervention} and concentrates resources '
        f'in {top_stage}. The leading province is {top_province}. The current portfolio should be adopted '
        f'only alongside a check on cascade bottlenecks, provincial implementation readiness, and equity '
        f'protection for disadvantaged strata.'
    )
    return {
        'policy_advisory_brief': national,
        'policy_operational_brief': operational,
        'policy_equity_brief': equity,
        'policy_budget_brief': budget,
        'policy_scenario_brief': scenario,
        'executive_summary': executive,
    }


def generate_province_briefs(grouped, disease):
    briefs = {}
    by_province = grouped.get('by_province', [])
    dominant_intervention = grouped.get('by_intervention', [{}])[0].get('name') if grouped.get('by_intervention') else 'the leading intervention'
    bottleneck = _province_bottleneck(grouped) or 'follow_up'
    for item in by_province:
        equity_concern = 'protect disadvantaged and remote populations'
        briefs[item['name']] = (
            f'Provincial advisory for {item["name"]}: this province is assigned ZAR {item["spend"]:,.0f} and is projected to gain '
            f'{item["dalys"]:,.1f} DALYs. The dominant intervention is {dominant_intervention}. '
            f'The likely bottleneck stage is {bottleneck}. A likely underuse reason is headroom or '
            f'eligible-population limits. The main equity concern is to {equity_concern} while scaling '
            f'the {disease.lower()} portfolio.'
        )
    return briefs


def build_scenario_tradeoff_table(results):
    table = []
    for name, payload in results.items():
        k = payload.get('kpis', {})
        eq = payload.get('equity_diagnostics', {})
        total_dalys = float(k.get('total_dalys_averted', 0) or 0)
        avg_cost = float(k.get('average_cost_per_daly', 0) or 0) if k.get('average_cost_per_daly') else None
        eq_share = float(eq.get('disadvantaged_spend_share', 0) or 0)
        balanced_score = total_dalys * 0.5 + eq_share * 100 * 0.3 + (0 if avg_cost in (None, 0) else (1 / avg_cost) * 1000 * 0.2)
        table.append({
            'scenario': name,
            'dalys_averted': total_dalys,
            'average_cost_per_daly': avg_cost,
            'disadvantaged_spend_share': eq_share,
            'balanced_score': round(balanced_score, 4),
        })
    return sorted(table, key=lambda x: x['dalys_averted'], reverse=True)


def generate_scenario_comparison_brief(results, disease):
    if not results:
        return 'No scenarios were run.'
    tradeoff = build_scenario_tradeoff_table(results)
    efficiency = sorted(tradeoff, key=lambda x: x['dalys_averted'], reverse=True)[0]
    equity = sorted(tradeoff, key=lambda x: x['disadvantaged_spend_share'], reverse=True)[0]
    balanced = sorted(tradeoff, key=lambda x: x['balanced_score'], reverse=True)[0]
    return (
        f'Scenario comparison advisory for {disease}. The best efficiency scenario is {efficiency["scenario"]}. '
        f'The best equity scenario is {equity["scenario"]}. The best balanced scenario is {balanced["scenario"]}.',
        tradeoff,
        efficiency['scenario'],
        equity['scenario'],
        balanced['scenario'],
    )
