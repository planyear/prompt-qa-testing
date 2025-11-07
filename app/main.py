# main.py (only the changes shown)

import os
import csv
import io
from fastapi import FastAPI, HTTPException, Form, UploadFile, File
from .schemas import RunJobResult, OutputRow
from .gdrive import (
    get_drive,
    extract_folder_id,
    download_file_by_name_from_folder,
    upload_bytes_to_folder_as_file,
)
from datetime import datetime
from .parser import parse_param_values_and_pages
from .comparer import HEADERS, compare_param_maps_rows   # <-- import HEADERS & rows helper

app = FastAPI(
    title="Prompt QA Tool",
    version="0.1.0",
    docs_url="/docs",
    redoc_url=None,
    swagger_ui_parameters={"defaultModelsExpandDepth": -1},
)

@app.post("/run", response_model=RunJobResult)
async def run_job(
    qa_guides_folder: str = Form(...),
    llm_outputs_folder: str = Form(...),
    output_folder: str = Form(...),
    mapping_csv_file: UploadFile = File(...),
):
    try:
        drive = get_drive()

        qa_guides_folder_id = extract_folder_id(qa_guides_folder)
        llm_outputs_folder_id = extract_folder_id(llm_outputs_folder)
        output_folder_id = extract_folder_id(output_folder)

        data = await mapping_csv_file.read()
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = data.decode("utf-8", errors="replace")

        dict_reader = csv.DictReader(io.StringIO(text))
        rows = [r for r in dict_reader]
        mapping_rows = []

        def _norm_pair(r):
            qa = r.get("qa_name") or r.get("QA_Name") or r.get("QA Guide") or r.get("qa")
            llm = r.get("llm_name") or r.get("LLM_Name") or r.get("LLM Output") or r.get("llm")
            return (qa.strip() if qa else ""), (llm.strip() if llm else "")

        ok_headers = False
        if dict_reader.fieldnames:
            names = [n.lower().strip() for n in dict_reader.fieldnames]
            if any(n in {"qa_name", "qa", "qa guide"} for n in names) and \
               any(n in {"llm_name", "llm", "llm output"} for n in names):
                ok_headers = True

        if ok_headers:
            for r in rows:
                qa, llm = _norm_pair(r)
                if qa or llm:
                    mapping_rows.append({"qa_name": qa, "llm_name": llm})
        else:
            reader = csv.reader(io.StringIO(text))
            for row in reader:
                if not any(c.strip() for c in row):
                    continue
                qa = (row[0] if len(row) > 0 else "").strip()
                llm = (row[1] if len(row) > 1 else "").strip()
                if qa or llm:
                    mapping_rows.append({"qa_name": qa, "llm_name": llm})

        if not mapping_rows:
            raise HTTPException(status_code=400, detail="Mapping CSV is empty or has no usable rows")

        # ---- NEW: one combined CSV buffer ----
        combined = io.StringIO()
        writer = csv.writer(combined)
        writer.writerow(HEADERS)

        # Build all rows across all pairs
        for idx, row in enumerate(mapping_rows, start=1):
            qa_name = (row.get("qa_name") or "").strip()
            llm_name = (row.get("llm_name") or "").strip()
            if not qa_name or not llm_name:
                raise HTTPException(status_code=400, detail=f"Row {idx} missing qa_name/llm_name")

            qa_text = download_file_by_name_from_folder(drive, qa_guides_folder_id, qa_name)
            llm_text = download_file_by_name_from_folder(drive, llm_outputs_folder_id, llm_name)

            qa_vals, qa_pages = parse_param_values_and_pages(qa_text)
            llm_vals, llm_pages = parse_param_values_and_pages(llm_text)

            for out_row in compare_param_maps_rows(
                qa_vals,
                llm_vals,
                qa_pages=qa_pages,
                llm_pages=llm_pages,
                qa_filename=os.path.basename(qa_name),
                llm_filename=os.path.basename(llm_name),
            ):
                writer.writerow(out_row)

        # Encode and upload ONE file (no filenames embedded)
        csv_bytes = combined.getvalue().encode("utf-8")
        csv_filename = f"prompt_qa_tool_output_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"

        uploaded = upload_bytes_to_folder_as_file(
            drive,
            parent_folder_id=output_folder_id,
            filename=csv_filename,
            mime_type="text/csv",
            data=csv_bytes,
        )

        # Return one OutputRow pointing to the single CSV
        outputs = [OutputRow(row=1, csv_file_id=uploaded["id"], csv_file_name=uploaded["name"])]
        return RunJobResult(outputs=outputs)

    except HTTPException:
        raise
