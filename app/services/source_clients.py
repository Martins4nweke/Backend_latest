from __future__ import annotations
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any
import pandas as pd
import requests
from app.services.taxonomy import gbd_search_terms, who_choice_bundle, who_gho_indicator_hints, country_code

UTC_NOW = lambda: datetime.now(timezone.utc).isoformat()

@dataclass
class EvidenceRecord:
    value: float | None
    parameter: str
    source_name: str
    source_type: str
    source_tier: str
    source_year: int | None
    retrieval_date: str
    fill_type: str
    confidence_flag: str
    notes: str = ""
    requested_year: int | None = None

    def to_provenance(self) -> dict[str, Any]:
        return {
            "selected_source": self.source_name,
            "source_type": self.source_type,
            "source_tier": self.source_tier,
            "requested_year": self.requested_year,
            "selected_year": self.source_year,
            "retrieval_date": self.retrieval_date,
            "fill_type": self.fill_type,
            "confidence_flag": self.confidence_flag,
            "notes": self.notes,
        }

class BaseClient:
    timeout = int(os.getenv("LIVE_SOURCE_TIMEOUT_SECONDS", "8"))

    def _get_json(self, url: str, params: dict[str, Any] | None = None):
        resp = requests.get(url, params=params, timeout=self.timeout, headers={"User-Agent": "ncd-backend-v6.3"})
        resp.raise_for_status()
        return resp.json()

    def _get_text(self, url: str, params: dict[str, Any] | None = None):
        resp = requests.get(url, params=params, timeout=self.timeout, headers={"User-Agent": "ncd-backend-v6.3"})
        resp.raise_for_status()
        return resp.text

    def _get_bytes(self, url: str, params: dict[str, Any] | None = None) -> bytes:
        resp = requests.get(url, params=params, timeout=self.timeout, headers={"User-Agent": "ncd-backend-v6.3"})
        resp.raise_for_status()
        return resp.content

class WorldBankPopulationClient(BaseClient):
    BASE = os.getenv("WORLD_BANK_API_URL", os.getenv("SA_HEALTH_API_URL", "https://api.worldbank.org/v2/country/{country}/indicator/SP.POP.TOTL"))

    def fetch(self, country_code_value: str, year: int | None = None) -> EvidenceRecord | None:
        if "{country}" in self.BASE:
            url = self.BASE.format(country=country_code_value.lower())
        else:
            base = self.BASE.rstrip("/")
            url = f"{base}/country/{country_code_value.lower()}/indicator/SP.POP.TOTL" if base.endswith("v2") else base
        params = {"format": "json", "per_page": 200}
        data = self._get_json(url, params=params)
        if not isinstance(data, list) or len(data) < 2:
            return None
        rows = [r for r in data[1] if r.get("value") is not None]
        if not rows:
            return None
        if year is not None:
            rows = sorted(rows, key=lambda r: abs(int(r.get("date", 0)) - int(year)))
            fill_type = "closest_year"
        else:
            rows = sorted(rows, key=lambda r: int(r.get("date", 0)), reverse=True)
            fill_type = "latest"
        top = rows[0]
        return EvidenceRecord(
            value=float(top["value"]), parameter="population", source_name="World Bank WDI",
            source_type="api", source_tier="global_relevant", source_year=int(top["date"]),
            retrieval_date=UTC_NOW(), fill_type=fill_type, confidence_flag="high", requested_year=year,
            notes="Population retrieved from World Bank indicator SP.POP.TOTL.",
        )

class GenericTabularUrlClient(BaseClient):
    def __init__(self, env_name: str):
        self.url = os.getenv(env_name, "").strip()

    def configured(self) -> bool:
        return bool(self.url)

    @lru_cache(maxsize=4)
    def read(self) -> pd.DataFrame | None:
        if not self.url:
            return None
        url = self.url.lower()
        if url.endswith(".csv"):
            return pd.read_csv(StringIO(self._get_text(self.url)))
        if url.endswith(".xlsx") or url.endswith(".xls"):
            return pd.read_excel(BytesIO(self._get_bytes(self.url)))
        return None

class CsvEvidencePackClient:
    def __init__(self):
        self.pack_dir = Path(os.getenv("COUNTRY_EVIDENCE_PACK_DIR", "")) if os.getenv("COUNTRY_EVIDENCE_PACK_DIR") else None

    def _load(self, filename: str) -> pd.DataFrame | None:
        if not self.pack_dir:
            return None
        path = self.pack_dir / filename
        if not path.exists():
            return None
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)
        return pd.read_excel(path)

    def fetch_gbd(self, country: str, disease: str, parameter: str, year: int | None = None) -> EvidenceRecord | None:
        df = self._load("gbd_latest.csv")
        if df is None:
            return None
        return match_gbd_dataframe(df, country, disease, parameter, year)

    def fetch_who_choice_cost(self, country: str, intervention: str, year: int | None = None) -> EvidenceRecord | None:
        df = self._load("who_choice_costs.csv")
        if df is None:
            return None
        return match_who_choice_dataframe(df, country, intervention, year)

    def fetch_country_metric(self, country: str, disease_or_key: str, parameter: str, year: int | None = None) -> EvidenceRecord | None:
        df = self._load("country_metrics.csv")
        if df is None:
            return None
        return match_country_metric_dataframe(df, country, disease_or_key, parameter, year, source_name_default="Country evidence pack", source_type="file_pack")

def _normalise_table(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work.columns = [str(c).strip().lower() for c in work.columns]
    return work


def _select_year(work: pd.DataFrame, year_col: str | None, requested_year: int | None) -> tuple[pd.DataFrame, str, int | None]:
    if year_col is None:
        return work, "exact", None
    filtered = work[pd.to_numeric(work[year_col], errors="coerce").notna()].copy()
    if filtered.empty:
        return work, "exact", None
    filtered[year_col] = pd.to_numeric(filtered[year_col], errors="coerce").astype(int)
    if requested_year is not None:
        exact = filtered[filtered[year_col] == int(requested_year)]
        if not exact.empty:
            return exact, "exact", int(requested_year)
        filtered = filtered.iloc[(filtered[year_col] - int(requested_year)).abs().argsort()]
        top_year = int(filtered.iloc[0][year_col])
        return filtered, "closest_year", top_year
    filtered = filtered.sort_values(year_col, ascending=False)
    top_year = int(filtered.iloc[0][year_col])
    return filtered, "latest", top_year


def match_country_metric_dataframe(df: pd.DataFrame, country: str, disease_or_key: str, parameter: str, year: int | None = None, source_name_default: str = "Country source", source_type: str = "csv_url") -> EvidenceRecord | None:
    work = _normalise_table(df)
    needed = {"country", "parameter", "value"}
    if not needed.issubset(set(work.columns)):
        return None
    mask = (work["country"].astype(str).str.lower() == str(country).lower()) & (work["parameter"].astype(str).str.lower() == str(parameter).lower())
    work = work[mask]
    if work.empty:
        return None
    disease_cols = [c for c in ["disease", "key", "concept", "intervention_name"] if c in work.columns]
    if disease_cols:
        dmask = False
        for col in disease_cols:
            dmask = dmask | (work[col].astype(str).str.lower() == str(disease_or_key).lower())
        if hasattr(dmask, "any") and dmask.any():
            work = work[dmask]
    if work.empty:
        return None
    year_col = next((c for c in ["year", "estimate_year", "year_id"] if c in work.columns), None)
    work, fill_type, selected_year = _select_year(work, year_col, year)
    row = work.iloc[0]
    val = float(row["value"])
    if parameter == "prevalence" and val > 1:
        val = val / 100.0
    return EvidenceRecord(
        value=val,
        parameter=parameter,
        source_name=str(row.get("source_name", source_name_default)),
        source_type=source_type,
        source_tier=str(row.get("source_tier", "country_secondary")),
        source_year=selected_year,
        retrieval_date=UTC_NOW(),
        fill_type=str(row.get("fill_type", fill_type)),
        confidence_flag=str(row.get("confidence_flag", "high")),
        notes=str(row.get("notes", "")),
        requested_year=year,
    )


def match_gbd_dataframe(df: pd.DataFrame, country: str, disease: str, parameter: str, year: int | None = None) -> EvidenceRecord | None:
    work = _normalise_table(df)
    terms = {t.lower() for t in gbd_search_terms(disease)}
    if "country" in work.columns:
        work = work[work["country"].astype(str).str.lower() == str(country).lower()]
    elif "location_name" in work.columns:
        work = work[work["location_name"].astype(str).str.lower() == str(country).lower()]
    if work.empty:
        return None

    if parameter == "daly_per_unit":
        if "measure" in work.columns:
            work = work[work["measure"].astype(str).str.lower().isin(["daly", "dalys", "disability-adjusted life years (dalys)", "dalys (disability-adjusted life years)"])]
    elif parameter == "prevalence":
        if "measure" in work.columns:
            work = work[work["measure"].astype(str).str.lower().isin(["prevalence"])]

    cause_col = next((c for c in ["cause", "cause_name", "rei_name", "indicator_name"] if c in work.columns), None)
    if cause_col:
        low_series = work[cause_col].astype(str).str.lower()
        work = work[low_series.isin(terms) | low_series.apply(lambda x: any(t in x for t in terms))]
    if work.empty:
        return None

    year_col = next((c for c in ["year", "year_id"] if c in work.columns), None)
    work, fill_type, selected_year = _select_year(work, year_col, year)
    row = work.iloc[0]
    value_col = next((c for c in ["value", "val", parameter, "dalys"] if c in work.columns), None)
    if value_col is None or pd.isna(row.get(value_col)):
        return None
    val = float(row[value_col])
    if parameter == "prevalence" and val > 1:
        val = val / 100.0
    notes = "Matched against a configured GBD dataset or export."
    if parameter == "daly_per_unit":
        notes += " This value may represent national disease burden rather than per-unit intervention effect and should be calibrated if programme effect data are available."
        fill_type = "proxy" if fill_type == "exact" else fill_type
    return EvidenceRecord(
        value=val, parameter=parameter, source_name="GBD configured source",
        source_type="csv_url", source_tier="global_relevant", source_year=selected_year, retrieval_date=UTC_NOW(),
        fill_type=fill_type, confidence_flag="moderate", requested_year=year, notes=notes,
    )


def match_who_choice_dataframe(df: pd.DataFrame, country: str, intervention: str, year: int | None = None) -> EvidenceRecord | None:
    work = _normalise_table(df)
    bundle = who_choice_bundle(intervention)
    if bundle is None:
        return None
    if "country" in work.columns:
        work = work[work["country"].astype(str).str.lower() == str(country).lower()]
    bundle_col = next((c for c in ["bundle", "bundle_name", "service_bundle", "action_bundle"] if c in work.columns), None)
    if bundle_col:
        low = work[bundle_col].astype(str).str.lower()
        work = work[low == bundle.lower()]
    if work.empty:
        return None
    year_col = next((c for c in ["year", "year_id"] if c in work.columns), None)
    work, fill_type, selected_year = _select_year(work, year_col, year)
    row = work.iloc[0]
    value_col = next((c for c in ["unit_cost_zar", "unit_cost", "cost", "value"] if c in work.columns), None)
    if value_col is None or pd.isna(row.get(value_col)):
        return None
    return EvidenceRecord(
        value=float(row[value_col]), parameter="unit_cost_zar", source_name="WHO-CHOICE configured source",
        source_type="csv_url", source_tier="global_relevant", source_year=selected_year, retrieval_date=UTC_NOW(),
        fill_type=fill_type, confidence_flag="moderate", requested_year=year,
        notes=f"WHO-CHOICE bundle match: {bundle}.",
    )

class WHOGHOClient(BaseClient):
    BASE = os.getenv("WHO_GHO_API_URL", "https://ghoapi.azureedge.net/api").rstrip("/")

    @lru_cache(maxsize=32)
    def find_indicator_code(self, disease: str) -> tuple[str, str] | None:
        hints = who_gho_indicator_hints(disease)
        try:
            data = self._get_json(f"{self.BASE}/Indicator")
        except Exception:
            return None
        items = data.get("value", data if isinstance(data, list) else [])
        best = None
        best_score = -1
        for item in items:
            name = str(item.get("IndicatorName", item.get("Title", "")))
            code = str(item.get("IndicatorCode", item.get("Code", "")))
            low = name.lower()
            score = 0
            if "prevalence" in low:
                score += 2
            for hint in hints:
                if hint.lower() in low:
                    score += 3
            if disease.lower() in low:
                score += 2
            if score > best_score and code:
                best_score = score
                best = (code, name)
        return best

    def _country_matches(self, row: dict[str, Any], country: str) -> bool:
        country = str(country).strip().lower()
        code = country_code(country)
        vals = [
            str(row.get("Title", "")).strip().lower(),
            str(row.get("Name", "")).strip().lower(),
            str(row.get("Display", "")).strip().lower(),
            str(row.get("Code", "")).strip().upper(),
            str(row.get("SpatialDim", "")).strip().upper(),
        ]
        if country in vals:
            return True
        if code and code in vals:
            return True
        return False

    def fetch_prevalence(self, country: str, disease: str, year: int | None = None) -> EvidenceRecord | None:
        indicator = self.find_indicator_code(disease)
        if not indicator:
            return None
        code, name = indicator
        try:
            data = self._get_json(f"{self.BASE}/{code}")
        except Exception:
            return None
        items = data.get("value", data if isinstance(data, list) else [])
        rows = [r for r in items if self._country_matches(r, country)]
        if not rows:
            return None
        def _year_of(r):
            for key in ["TimeDim", "TimeDimensionBegin", "Year"]:
                try:
                    return int(str(r.get(key))[:4])
                except Exception:
                    pass
            return 0
        if year is not None:
            rows = sorted(rows, key=lambda r: abs(_year_of(r) - int(year)))
            fill_type = "closest_year"
        else:
            rows = sorted(rows, key=_year_of, reverse=True)
            fill_type = "latest"
        top = rows[0]
        raw = top.get("NumericValue", top.get("Value", top.get("FactValueNumeric", top.get("FactValue", None))))
        if raw is None or pd.isna(raw):
            return None
        value = float(raw)
        if value > 1:
            value = value / 100.0
        src_year = _year_of(top) or None
        return EvidenceRecord(
            value=value, parameter="prevalence", source_name=f"WHO GHO: {name}",
            source_type="api", source_tier="global_relevant", source_year=src_year, retrieval_date=UTC_NOW(),
            fill_type=fill_type, confidence_flag="moderate", requested_year=year,
            notes="Prevalence retrieved from WHO GHO OData API and normalised to a 0-1 proportion where needed.",
        )

class SourceReadiness:
    def status(self) -> dict[str, Any]:
        pack_dir = os.getenv("COUNTRY_EVIDENCE_PACK_DIR", "")
        gbd_url = os.getenv("GBD_API_URL", "").strip() or os.getenv("GBD_CSV_URL", "").strip()
        who_choice_url = os.getenv("WHO_CHOICE_API_URL", "").strip() or os.getenv("WHO_CHOICE_CSV_URL", "").strip()
        who_gho_url = os.getenv("WHO_GHO_API_URL", "").strip() or os.getenv("WHO_GHO_CSV_URL", "").strip()
        country_url = os.getenv("COUNTRY_SOURCE_URL", "").strip() or os.getenv("SA_HEALTH_API_URL", "").strip()
        return {
            "world_bank_population": {"configured": True, "mode": "api", "url": WorldBankPopulationClient.BASE},
            "country_evidence_pack": {"configured": bool(pack_dir), "mode": "file_pack", "path": pack_dir or None},
            "country_source": {"configured": bool(country_url), "mode": "csv_url" if country_url.lower().endswith((".csv", ".xlsx", ".xls")) else "api_or_catalog", "url": country_url or None},
            "gbd_source": {"configured": bool(gbd_url), "mode": "csv_url" if gbd_url.lower().endswith((".csv", ".xlsx", ".xls")) else "catalog_url", "url": gbd_url or None},
            "who_choice_source": {"configured": bool(who_choice_url), "mode": "csv_url" if who_choice_url.lower().endswith((".csv", ".xlsx", ".xls")) else "catalog_url", "url": who_choice_url or None},
            "who_gho_source": {"configured": bool(who_gho_url), "mode": "odata_api" if who_gho_url else None, "url": who_gho_url or None},
        }
