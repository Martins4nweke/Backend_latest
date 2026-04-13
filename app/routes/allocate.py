from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse
from io import BytesIO
from app.services.excel_reader import read_csv_or_excel
from app.services.preprocessing import standardise_dataframe, prepare_allocation_dataframe, apply_population_basis_rule
from app.services.validation import validate_input_dataframe, summarise_missingness, summarise_provenance
from app.services.dataset_builder import assemble_minimum_dataset
from app.services.imputation import simple_impute
from app.services.allocation import rows_from_dataframe, greedy_allocate, summarise_kpis
from app.services.grouping import build_grouped_summaries
from app.services.diagnostics import compute_equity_metrics, compute_budget_diagnostics, compute_structure_diagnostics, compute_parameter_provenance_summary
from app.services.policy import generate_policy_bundle, generate_equity_recommendations, generate_province_briefs, generate_scenario_comparison_brief, build_province_comparative_table
from app.services.reporting import build_word_report
from app.services.evidence_retrieval import retrieve_public_evidence, build_parameter_provenance_preview
from app.services.source_clients import SourceReadiness
router = APIRouter()

def _scenario_weights(name):
    if name == 'screening_heavy':
        return {'screening': 1.2}
    if name == 'treatment_heavy':
        return {'treatment_initiation': 1.2, 'follow_up': 1.2}
    if name == 'balanced_cascade':
        return {s: 1.0 for s in ['screening', 'diagnosis', 'treatment_initiation', 'follow_up', 'adherence']}
    return None

def _make_limitations_note(parameter_prov_summary, budget_diag, evidence_summary):
    parts = []
    if parameter_prov_summary.get('rows_with_imputed_max_coverage', 0) > 0:
        parts.append('Some rows relied on stage-based default maximum coverage rather than directly observed programme ceilings.')
    if parameter_prov_summary.get('rows_with_imputed_daly', 0) > 0:
        parts.append('Some DALY effects were imputed and should be interpreted cautiously.')
    if budget_diag.get('budget_underuse_flag'):
        parts.append('Budget underutilisation suggests that allocation constraints other than funding may be binding.')
    if evidence_summary.get('filled_from_public_sources', 0) > 0:
        parts.append('Some missing parameters were filled from public sources at runtime and should be reviewed for context fit before policy adoption.')
    return ' '.join(parts) if parts else 'Most core parameters were provided directly and the allocation used the available budget envelope as expected.'

def _make_confidence_note(parameter_prov_summary):
    imputed_total = parameter_prov_summary.get('rows_with_imputed_max_coverage', 0) + parameter_prov_summary.get('rows_with_imputed_daly', 0) + parameter_prov_summary.get('rows_with_imputed_cost', 0) + parameter_prov_summary.get('rows_with_imputed_prevalence', 0)
    if imputed_total == 0:
        return 'Confidence is relatively strong because the core model inputs were supplied directly or retrieved from ranked public sources.'
    if imputed_total < 20:
        return 'Confidence is moderate because a limited number of model inputs relied on fallback or imputed values.'
    return 'Confidence is moderate to cautious because a material share of rows relied on fallback or imputed values.'

def _run_from_df(df, budget_zar, apply_imputation=False, equity_weights=None, scenario_stage_weights=None, scenario_name='baseline'):
    df = standardise_dataframe(df)
    df, population_basis_summary = apply_population_basis_rule(df)
    missingness_summary = summarise_missingness(df)
    df, evidence_retrieval_summary = retrieve_public_evidence(df)
    df, assembly_summary = assemble_minimum_dataset(df)
    validation_errors, warnings = validate_input_dataframe(df)
    if validation_errors and not apply_imputation:
        return {
            'metadata': {'budget_zar': budget_zar, 'apply_imputation': apply_imputation, 'scenario_name': scenario_name, 'status': 'validation_failed'},
            'validation_errors': validation_errors, 'warnings': warnings, 'missingness_summary': missingness_summary,
            'provenance_summary': summarise_provenance(df) | {'assembly_summary': assembly_summary}, 'imputation_summary': {},
            'evidence_retrieval_summary': evidence_retrieval_summary, 'parameter_provenance_preview': build_parameter_provenance_preview(df),
            'kpis': {'budget_zar': budget_zar, 'total_spend_zar': 0, 'budget_remaining_zar': budget_zar, 'total_dalys_averted': 0, 'total_units_allocated': 0, 'average_cost_per_daly': None},
            'policy_advisory_brief': 'Validation failed. No policy brief generated.', 'policy_operational_brief': '', 'policy_equity_brief': '', 'policy_budget_brief': '', 'policy_scenario_brief': '',
            'equity_recommendations': [], 'province_briefs': {}, 'grouped_summaries': {'by_intervention': [], 'by_province': [], 'by_stratum': [], 'by_stage': [], 'top_rows': []}, 'allocation_results': [],
        }
    imputation_summary = {}
    if apply_imputation:
        df, imputation_summary = simple_impute(df)
        validation_errors, new_warnings = validate_input_dataframe(df)
        warnings.extend([w for w in new_warnings if w not in warnings])
        if validation_errors:
            raise HTTPException(status_code=400, detail={'validation_errors': validation_errors})
    prepared = prepare_allocation_dataframe(df)
    rows = rows_from_dataframe(prepared)
    allocations, budget_remaining = greedy_allocate(rows=rows, total_budget=budget_zar, equity_weights=equity_weights, scenario_stage_weights=scenario_stage_weights)
    kpis = summarise_kpis(allocations, budget_zar, budget_remaining)
    grouped = build_grouped_summaries(allocations)
    disease = str(df['disease'].iloc[0]) if 'disease' in df.columns and len(df) else 'NCD'
    equity_diag = compute_equity_metrics(allocations)
    budget_diag = compute_budget_diagnostics(kpis, grouped, allocations)
    structure_diag = compute_structure_diagnostics(grouped)
    parameter_prov_summary = compute_parameter_provenance_summary(allocations)
    policy_bundle = generate_policy_bundle(kpis, grouped, allocations, disease, budget_diag, structure_diag, equity_diag)
    province_table = build_province_comparative_table(grouped, disease)
    return {
        'metadata': {'budget_zar': budget_zar, 'apply_imputation': apply_imputation, 'scenario_name': scenario_name, 'rows_received': int(len(df)), 'rows_ranked': int(len(rows)), 'status': 'success', **population_basis_summary},
        'validation_errors': validation_errors,
        'warnings': warnings,
        'missingness_summary': missingness_summary,
        'provenance_summary': summarise_provenance(df) | {'assembly_summary': assembly_summary},
        'parameter_provenance_summary': parameter_prov_summary,
        'parameter_provenance_preview': build_parameter_provenance_preview(df),
        'evidence_retrieval_summary': evidence_retrieval_summary,
        'imputation_summary': imputation_summary,
        'kpis': kpis,
        'budget_diagnostics': budget_diag,
        'structure_diagnostics': structure_diag,
        'equity_diagnostics': equity_diag,
        **policy_bundle,
        'equity_recommendations': generate_equity_recommendations(allocations, grouped),
        'province_briefs': generate_province_briefs(grouped, disease),
        'province_comparative_table': province_table,
        'grouped_summaries': grouped,
        'limitations_note': _make_limitations_note(parameter_prov_summary, budget_diag, evidence_retrieval_summary),
        'confidence_note': _make_confidence_note(parameter_prov_summary),
        'allocation_results': allocations,
    }

@router.get('/source-readiness')
def source_readiness():
    return SourceReadiness().status()

@router.post('/evidence-fill-preview')
async def evidence_fill_preview(file: UploadFile = File(...)):
    content = await file.read()
    try:
        df = read_csv_or_excel(content, file.filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    df = standardise_dataframe(df)
    filled, summary = retrieve_public_evidence(df)
    return {'metadata': {'filename': file.filename}, 'evidence_retrieval_summary': summary, 'parameter_provenance_preview': build_parameter_provenance_preview(filled, limit=50)}

@router.post('/allocate')
async def allocate(file: UploadFile = File(...), budget_zar: float = Form(...), apply_imputation: bool = Form(False)):
    if budget_zar <= 0:
        raise HTTPException(status_code=400, detail='budget_zar must be greater than 0.')
    content = await file.read()
    try:
        df = read_csv_or_excel(content, file.filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    payload = _run_from_df(df, budget_zar=budget_zar, apply_imputation=apply_imputation)
    payload['metadata']['filename'] = file.filename
    return payload

@router.post('/compare-scenarios')
async def compare_scenarios(file: UploadFile = File(...), budget_zar: float = Form(...), apply_imputation: bool = Form(False)):
    if budget_zar <= 0:
        raise HTTPException(status_code=400, detail='budget_zar must be greater than 0.')
    content = await file.read()
    try:
        df = read_csv_or_excel(content, file.filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    scenarios = {
        'baseline': {'budget': budget_zar, 'equity': None, 'stage': None},
        'equity_sensitive': {'budget': budget_zar, 'equity': {'S1': 1.3, 'S2': 1.2}, 'stage': None},
        'budget_cut_15': {'budget': budget_zar * 0.85, 'equity': None, 'stage': None},
        'screening_heavy': {'budget': budget_zar, 'equity': None, 'stage': _scenario_weights('screening_heavy')},
        'treatment_heavy': {'budget': budget_zar, 'equity': None, 'stage': _scenario_weights('treatment_heavy')},
        'balanced_cascade': {'budget': budget_zar, 'equity': None, 'stage': _scenario_weights('balanced_cascade')},
    }
    results = {}
    for name, cfg in scenarios.items():
        results[name] = _run_from_df(df.copy(), budget_zar=cfg['budget'], apply_imputation=apply_imputation, equity_weights=cfg['equity'], scenario_stage_weights=cfg['stage'], scenario_name=name)
    disease = str(df['disease'].iloc[0]) if 'disease' in df.columns and len(df) else 'NCD'
    scenario_brief, tradeoff_table, best_efficiency, best_equity, best_balanced = generate_scenario_comparison_brief(results, disease)
    return {'metadata': {'filename': file.filename, 'base_budget_zar': budget_zar, 'apply_imputation': apply_imputation, 'scenario_count': len(results), 'disease': disease}, 'scenario_results': results, 'scenario_comparison_brief': scenario_brief, 'scenario_tradeoff_table': tradeoff_table, 'best_efficiency_scenario': best_efficiency, 'best_equity_scenario': best_equity, 'best_balanced_scenario': best_balanced}

@router.post('/export-word')
async def export_word(file: UploadFile = File(...), budget_zar: float = Form(...), apply_imputation: bool = Form(False)):
    if budget_zar <= 0:
        raise HTTPException(status_code=400, detail='budget_zar must be greater than 0.')
    content = await file.read()
    try:
        df = read_csv_or_excel(content, file.filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    payload = _run_from_df(df, budget_zar=budget_zar, apply_imputation=apply_imputation)
    scenarios = {
        'baseline': {'budget': budget_zar, 'equity': None, 'stage': None},
        'equity_sensitive': {'budget': budget_zar, 'equity': {'S1': 1.3, 'S2': 1.2}, 'stage': None},
        'budget_cut_15': {'budget': budget_zar * 0.85, 'equity': None, 'stage': None},
        'screening_heavy': {'budget': budget_zar, 'equity': None, 'stage': _scenario_weights('screening_heavy')},
        'treatment_heavy': {'budget': budget_zar, 'equity': None, 'stage': _scenario_weights('treatment_heavy')},
        'balanced_cascade': {'budget': budget_zar, 'equity': None, 'stage': _scenario_weights('balanced_cascade')},
    }
    results = {}
    for name, cfg in scenarios.items():
        results[name] = _run_from_df(df.copy(), budget_zar=cfg['budget'], apply_imputation=apply_imputation, equity_weights=cfg['equity'], scenario_stage_weights=cfg['stage'], scenario_name=name)
    disease = str(df['disease'].iloc[0]) if 'disease' in df.columns and len(df) else 'NCD'
    scenario_brief, tradeoff_table, best_efficiency, best_equity, best_balanced = generate_scenario_comparison_brief(results, disease)
    payload['scenario_tradeoff_table'] = tradeoff_table
    payload['policy_scenario_brief'] = scenario_brief
    payload['best_efficiency_scenario'] = best_efficiency
    payload['best_equity_scenario'] = best_equity
    payload['best_balanced_scenario'] = best_balanced
    docx_bytes = build_word_report(payload)
    return StreamingResponse(BytesIO(docx_bytes), media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document', headers={'Content-Disposition': 'attachment; filename=ncd_policy_report_v63.docx'})
