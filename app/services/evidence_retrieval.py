from __future__ import annotations
from datetime import datetime, timezone
from typing import Any
import pandas as pd
from app.services.source_clients import (
    WorldBankPopulationClient, CsvEvidencePackClient, EvidenceRecord, SourceReadiness,
    GenericTabularUrlClient, WHOGHOClient, match_gbd_dataframe, match_who_choice_dataframe,
    match_country_metric_dataframe,
)
from app.services.taxonomy import canonical_disease, canonical_intervention, country_code

UTC_NOW = lambda: datetime.now(timezone.utc).isoformat()

PARAMETERS = ["population", "prevalence", "baseline_coverage", "max_coverage", "unit_cost_zar", "daly_per_unit"]


def _append(existing: Any, text: str) -> str:
    if existing is None or pd.isna(existing) or str(existing).strip() in {"", "<NA>", "nan"}:
        return text
    current = str(existing)
    return current if text in current else f"{current}; {text}"


def _is_missing(value: Any) -> bool:
    return pd.isna(value) or str(value).strip() == ""


def _year(row) -> int | None:
    for key in ["year", "estimate_year"]:
        try:
            if pd.notna(row.get(key)):
                return int(row.get(key))
        except Exception:
            pass
    return None


def _set_param(work: pd.DataFrame, idx: int, param: str, record: EvidenceRecord):
    work.at[idx, param] = record.value
    work.at[idx, f"{param}_source_name"] = record.source_name
    work.at[idx, f"{param}_source_type"] = record.source_type
    work.at[idx, f"{param}_source_tier"] = record.source_tier
    work.at[idx, f"{param}_is_imputed"] = record.fill_type == "imputed"
    work.at[idx, f"{param}_source_year"] = record.source_year
    work.at[idx, f"{param}_selected_year"] = record.source_year
    work.at[idx, f"{param}_requested_year"] = record.requested_year
    work.at[idx, f"{param}_retrieval_date"] = record.retrieval_date
    work.at[idx, f"{param}_fill_type"] = record.fill_type
    work.at[idx, f"{param}_confidence_flag"] = record.confidence_flag
    work.at[idx, "notes"] = _append(work.at[idx, "notes"] if "notes" in work.columns else None, f"{param} filled from {record.source_name} ({record.fill_type}).")


def _populate_default_metadata_columns(work: pd.DataFrame):
    extra_suffixes = ["source_name", "source_type", "source_tier", "is_imputed", "source_year", "selected_year", "requested_year", "retrieval_date", "fill_type", "confidence_flag"]
    for param in PARAMETERS:
        for suffix in extra_suffixes:
            col = f"{param}_{suffix}"
            if col not in work.columns:
                work[col] = pd.NA
    if "notes" not in work.columns:
        work["notes"] = pd.NA
    if "evidence_strategy" not in work.columns:
        work["evidence_strategy"] = "uploaded_then_country_then_relevant_global_then_proxy_then_impute"
    return work


def _fill_population(row, world_bank: WorldBankPopulationClient, pack: CsvEvidencePackClient, country_source: GenericTabularUrlClient) -> EvidenceRecord | None:
    country = str(row.get("country", "")).strip()
    disease = canonical_disease(row.get("disease"))
    year = _year(row)
    rec = pack.fetch_country_metric(country, disease, "population", year=year)
    if rec is not None:
        return rec
    df = country_source.read()
    if df is not None:
        rec = match_country_metric_dataframe(df, country, disease, "population", year, source_name_default="Configured country source", source_type="csv_url")
        if rec is not None:
            return rec
    code = country_code(country)
    if code:
        try:
            return world_bank.fetch(code, year=year)
        except Exception:
            return None
    return None


def _fill_prevalence(row, pack: CsvEvidencePackClient, who_gho: WHOGHOClient, who_gho_csv: GenericTabularUrlClient, country_source: GenericTabularUrlClient, gbd_csv: GenericTabularUrlClient) -> EvidenceRecord | None:
    country = str(row.get("country", "")).strip()
    disease = canonical_disease(row.get("disease"))
    year = _year(row)

    rec = pack.fetch_country_metric(country, disease, "prevalence", year=year)
    if rec is not None:
        return rec

    df = country_source.read()
    if df is not None:
        rec = match_country_metric_dataframe(df, country, disease, "prevalence", year, source_name_default="Configured country source", source_type="csv_url")
        if rec is not None:
            return rec

    live = who_gho.fetch_prevalence(country, disease, year=year)
    if live is not None:
        return live

    csv_df = who_gho_csv.read()
    if csv_df is not None:
        rec = match_country_metric_dataframe(csv_df.rename(columns={c: c for c in csv_df.columns}), country, disease, "prevalence", year, source_name_default="WHO GHO configured dataset", source_type="csv_url")
        if rec is not None:
            if rec.value and rec.value > 1:
                rec.value = rec.value / 100.0
            rec.source_tier = "global_relevant"
            return rec

    rec = pack.fetch_gbd(country, disease, "prevalence", year=year)
    if rec is not None:
        rec.source_name = "GBD evidence pack"
        rec.source_tier = "global_relevant"
        return rec

    df = gbd_csv.read()
    if df is not None:
        rec = match_gbd_dataframe(df, country, disease, "prevalence", year)
        if rec is not None:
            return rec
    return None


def _fill_daly(row, pack: CsvEvidencePackClient, gbd_csv: GenericTabularUrlClient, country_source: GenericTabularUrlClient) -> EvidenceRecord | None:
    country = str(row.get("country", "")).strip()
    disease = canonical_disease(row.get("disease"))
    year = _year(row)

    rec = pack.fetch_country_metric(country, disease, "daly_per_unit", year=year)
    if rec is not None:
        return rec

    df = country_source.read()
    if df is not None:
        rec = match_country_metric_dataframe(df, country, disease, "daly_per_unit", year, source_name_default="Configured country source", source_type="csv_url")
        if rec is not None:
            return rec

    rec = pack.fetch_gbd(country, disease, "daly_per_unit", year=year)
    if rec is not None:
        rec.notes = rec.notes + " This value may represent national burden rather than per-unit intervention effect and should be calibrated if programme effect data are available."
        rec.fill_type = "proxy" if rec.fill_type == "exact" else rec.fill_type
        rec.confidence_flag = "moderate"
        return rec

    df = gbd_csv.read()
    if df is not None:
        rec = match_gbd_dataframe(df, country, disease, "daly_per_unit", year)
        if rec is not None:
            return rec
    return None


def _fill_cost(row, pack: CsvEvidencePackClient, who_choice_csv: GenericTabularUrlClient, country_source: GenericTabularUrlClient) -> EvidenceRecord | None:
    country = str(row.get("country", "")).strip()
    intervention = canonical_intervention(row.get("intervention_name"))
    year = _year(row)

    rec = pack.fetch_country_metric(country, intervention, "unit_cost_zar", year=year)
    if rec is not None:
        return rec

    df = country_source.read()
    if df is not None:
        rec = match_country_metric_dataframe(df, country, intervention, "unit_cost_zar", year, source_name_default="Configured country source", source_type="csv_url")
        if rec is not None:
            return rec

    rec = pack.fetch_who_choice_cost(country, intervention, year=year)
    if rec is not None:
        rec.fill_type = "proxy" if rec.fill_type == "exact" else rec.fill_type
        rec.confidence_flag = "moderate"
        return rec

    df = who_choice_csv.read()
    if df is not None:
        rec = match_who_choice_dataframe(df, country, intervention, year)
        if rec is not None:
            return rec
    return None


def retrieve_public_evidence(df: pd.DataFrame):
    work = _populate_default_metadata_columns(df.copy())
    pack = CsvEvidencePackClient()
    wb = WorldBankPopulationClient()
    country_source = GenericTabularUrlClient("COUNTRY_SOURCE_URL") if GenericTabularUrlClient("COUNTRY_SOURCE_URL").configured() else GenericTabularUrlClient("SA_HEALTH_API_URL")
    gbd_csv = GenericTabularUrlClient("GBD_API_URL") if GenericTabularUrlClient("GBD_API_URL").configured() else GenericTabularUrlClient("GBD_CSV_URL")
    who_choice_csv = GenericTabularUrlClient("WHO_CHOICE_API_URL") if GenericTabularUrlClient("WHO_CHOICE_API_URL").configured() else GenericTabularUrlClient("WHO_CHOICE_CSV_URL")
    who_gho_csv = GenericTabularUrlClient("WHO_GHO_CSV_URL")
    who_gho = WHOGHOClient()
    summary = {
        "rows_scanned": int(len(work)),
        "filled_from_external_sources": 0,
        "by_parameter": {"population": 0, "prevalence": 0, "unit_cost_zar": 0, "daly_per_unit": 0},
        "retrieval_mode": "uploaded_then_country_then_relevant_global_then_impute",
        "source_readiness": SourceReadiness().status(),
    }
    for idx, row in work.iterrows():
        if _is_missing(row.get("population")):
            rec = _fill_population(row, wb, pack, country_source)
            if rec and rec.value is not None:
                _set_param(work, idx, "population", rec)
                summary["filled_from_external_sources"] += 1
                summary["by_parameter"]["population"] += 1
        if _is_missing(row.get("prevalence")):
            rec = _fill_prevalence(row, pack, who_gho, who_gho_csv, country_source, gbd_csv)
            if rec and rec.value is not None:
                _set_param(work, idx, "prevalence", rec)
                summary["filled_from_external_sources"] += 1
                summary["by_parameter"]["prevalence"] += 1
        if _is_missing(row.get("unit_cost_zar")):
            rec = _fill_cost(row, pack, who_choice_csv, country_source)
            if rec and rec.value is not None:
                _set_param(work, idx, "unit_cost_zar", rec)
                summary["filled_from_external_sources"] += 1
                summary["by_parameter"]["unit_cost_zar"] += 1
        if _is_missing(row.get("daly_per_unit")):
            rec = _fill_daly(row, pack, gbd_csv, country_source)
            if rec and rec.value is not None:
                _set_param(work, idx, "daly_per_unit", rec)
                summary["filled_from_external_sources"] += 1
                summary["by_parameter"]["daly_per_unit"] += 1
    return work, summary


def build_parameter_provenance_preview(df: pd.DataFrame, limit: int = 12) -> list[dict[str, Any]]:
    out = []
    params = ["population", "prevalence", "unit_cost_zar", "daly_per_unit"]
    for _, row in df.head(limit).iterrows():
        item = {
            "disease": row.get("disease"),
            "country": row.get("country"),
            "province": row.get("province"),
            "intervention_name": row.get("intervention_name"),
            "parameters": {},
        }
        for p in params:
            item["parameters"][p] = {
                "value": None if pd.isna(row.get(p)) else row.get(p),
                "originally_missing": bool(pd.notna(row.get(f"{p}_fill_type"))),
                "selected_source": None if pd.isna(row.get(f"{p}_source_name")) else row.get(f"{p}_source_name"),
                "source_type": None if pd.isna(row.get(f"{p}_source_type")) else row.get(f"{p}_source_type"),
                "source_tier": None if pd.isna(row.get(f"{p}_source_tier")) else row.get(f"{p}_source_tier"),
                "requested_year": None if pd.isna(row.get(f"{p}_requested_year")) else row.get(f"{p}_requested_year"),
                "selected_year": None if pd.isna(row.get(f"{p}_selected_year")) else row.get(f"{p}_selected_year"),
                "fill_type": None if pd.isna(row.get(f"{p}_fill_type")) else row.get(f"{p}_fill_type"),
                "confidence_flag": None if pd.isna(row.get(f"{p}_confidence_flag")) else row.get(f"{p}_confidence_flag"),
            }
        out.append(item)
    return out
