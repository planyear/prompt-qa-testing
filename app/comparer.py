# comparer.py
import csv
import io
from typing import Dict, Tuple, List

HEADERS = [
    "QA File Name",                 # NEW
    "PTO File Name",                # NEW
    "QA Parameter Name",            # rename of "Parameter"
    "QA Parameter Value",           # rename of "QA Guide Value"
    "PTO Parameter Value",          # rename of "Prompt Tuner Value"
    "Parameter Value Match",        # rename of "True/False"
    "QA Parameter Page Number",     # NEW
    "PTO Parameter Page Number",    # NEW
    "Parameter Page Number Match",  # NEW
]

def compare_param_maps_rows(
    qa_values: Dict[str, str],
    llm_values: Dict[str, str],
    *,
    qa_pages: Dict[str, str] | None = None,
    llm_pages: Dict[str, str] | None = None,
    qa_filename: str = "",
    llm_filename: str = "",
) -> List[List[str]]:
    """Return rows (matching HEADERS) for a single QA/PTO pair."""
    qa_pages = qa_pages or {}
    llm_pages = llm_pages or {}
    rows: List[List[str]] = []

    for k in sorted(qa_values.keys()):
        qa_v = (qa_values.get(k) or "").strip()
        llm_v = (llm_values.get(k) or "").strip()
        value_match = str(qa_v == llm_v)

        qa_p = (qa_pages.get(k) or "").strip()
        llm_p = (llm_pages.get(k) or "").strip()
        page_match = str(qa_p == llm_p)

        rows.append([
            qa_filename,
            llm_filename,
            k,          # QA Parameter Name
            qa_v,
            llm_v,
            value_match,
            qa_p,
            llm_p,
            page_match,
        ])
    return rows


# keep the old function for compatibility (now implemented via rows)
def compare_param_maps_to_csv(
    qa_values: Dict[str, str],
    llm_values: Dict[str, str],
    output_basename: str,
    *,
    qa_pages: Dict[str, str] | None = None,
    llm_pages: Dict[str, str] | None = None,
    qa_filename: str = "",
    llm_filename: str = "",
) -> Tuple[bytes, str]:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(HEADERS)
    for row in compare_param_maps_rows(
        qa_values, llm_values,
        qa_pages=qa_pages, llm_pages=llm_pages,
        qa_filename=qa_filename, llm_filename=llm_filename,
    ):
        writer.writerow(row)
    csv_bytes = buf.getvalue().encode("utf-8")
    filename = f"{output_basename}__qa_vs_llm.csv"
    return csv_bytes, filename
