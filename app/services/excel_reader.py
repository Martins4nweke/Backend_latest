from io import BytesIO
import pandas as pd
CORE = {"disease","province","stratum_code","cascade_stage","intervention_name","population","prevalence","baseline_coverage","max_coverage","unit_cost_zar","daly_per_unit"}

def _norm(cols):
    return [str(c).strip().lower() for c in cols]

def _has_core(df):
    return CORE.issubset(set(_norm(df.columns)))

def _find_header_row(raw, max_scan_rows=25):
    limit = min(max_scan_rows, len(raw))
    for idx in range(limit):
        row_vals = [str(v).strip().lower() for v in raw.iloc[idx].tolist()]
        if CORE.issubset(set(row_vals)):
            return idx
    return None

def read_csv_or_excel(upload_bytes: bytes, filename: str) -> pd.DataFrame:
    name = filename.lower()
    if name.endswith('.csv'):
        return pd.read_csv(BytesIO(upload_bytes))
    if name.endswith('.xlsx') or name.endswith('.xls'):
        excel = pd.ExcelFile(BytesIO(upload_bytes))
        for sheet in excel.sheet_names:
            df = pd.read_excel(BytesIO(upload_bytes), sheet_name=sheet)
            if _has_core(df):
                return df
        for sheet in excel.sheet_names:
            raw = pd.read_excel(BytesIO(upload_bytes), sheet_name=sheet, header=None)
            h = _find_header_row(raw)
            if h is not None:
                df = pd.read_excel(BytesIO(upload_bytes), sheet_name=sheet, header=h)
                if _has_core(df):
                    return df
        return pd.read_excel(BytesIO(upload_bytes), sheet_name=excel.sheet_names[0])
    raise ValueError('Only CSV and Excel files are supported.')
