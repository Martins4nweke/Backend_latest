def aggregate_by_key(rows, key):
    grouped = {}
    total_spend = sum(float(r.get('spend_zar', 0)) for r in rows)
    total_dalys = sum(float(r.get('dalys_averted', 0)) for r in rows)
    for row in rows:
        name = row.get(key, 'Unknown')
        if name not in grouped:
            grouped[name] = {'name': name, 'spend': 0.0, 'dalys': 0.0, 'units': 0.0, 'rows': 0}
        grouped[name]['spend'] += float(row.get('spend_zar', 0))
        grouped[name]['dalys'] += float(row.get('dalys_averted', 0))
        grouped[name]['units'] += float(row.get('units_allocated', 0))
        grouped[name]['rows'] += 1
    out = []
    for item in grouped.values():
        item['spend_share'] = round((item['spend'] / total_spend), 4) if total_spend > 0 else 0
        item['daly_share'] = round((item['dalys'] / total_dalys), 4) if total_dalys > 0 else 0
        item['cost_per_daly'] = round((item['spend'] / item['dalys']), 4) if item['dalys'] > 0 else None
        out.append(item)
    out = sorted(out, key=lambda x: x['spend'], reverse=True)
    for i, item in enumerate(out, start=1):
        item['rank'] = i
    return out

def build_grouped_summaries(rows):
    return {
        'by_intervention': aggregate_by_key(rows, 'intervention_name'),
        'by_province': aggregate_by_key(rows, 'province'),
        'by_stratum': aggregate_by_key(rows, 'stratum_code'),
        'by_stage': aggregate_by_key(rows, 'cascade_stage'),
        'top_rows': sorted(rows, key=lambda x: float(x.get('spend_zar', 0)), reverse=True)[:12],
    }
