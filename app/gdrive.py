import csv
import io
import json
import os
import re
import difflib
import unicodedata
from typing import Dict, List, Optional

# --- load .env robustly from project root ---
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=(ENV_PATH if ENV_PATH.exists() else find_dotenv(usecwd=True)), override=True)

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.file",
]

SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

def extract_folder_id(link_or_id: str) -> str:
    """Accepts Drive folder/file links or a bare ID and returns the ID."""
    s = (link_or_id or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{10,}", s):  # bare id
        return s
    m = re.search(r"/folders/([A-Za-z0-9_-]{10,})", s) or \
        re.search(r"[?&]id=([A-Za-z0-9_-]{10,})", s) or \
        re.search(r"/file/d/([A-Za-z0-9_-]{10,})", s)
    if m:
        return m.group(1)
    raise ValueError(f"Could not extract a Google Drive folder ID from: {link_or_id!r}")

def get_drive():
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env var is required (JSON string or path)")
    s = SERVICE_ACCOUNT_JSON.strip()
    if s.startswith("{"):
        info = json.loads(s)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = service_account.Credentials.from_service_account_file(s, scopes=SCOPES)
    # Helpful log once:
    print("Using service account:", creds.service_account_email)
    return build("drive", "v3", credentials=creds)

# -------- tolerant name matching (handles NBSP, case, .txt suffix) --------
def _normalize_name(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u00A0", " ")  # NBSP -> space
    s = " ".join(s.split())       # collapse whitespace
    return s.strip().lower()

def _find_file_in_folder_by_name(drive, folder_id: str, name: str) -> Optional[Dict]:
    # 1) Exact query first (fast path)
    safe_name = name.replace("'", "\\'")
    q = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
    res = drive.files().list(
        q=q,
        spaces="drive",
        fields="files(id, name, mimeType)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="allDrives",
        pageSize=100,
    ).execute()
    files = res.get("files", [])
    if files:
        return files[0]

    # 2) List children and try tolerant match
    res = drive.files().list(
        q=f"'{folder_id}' in parents and trashed = false",
        spaces="drive",
        fields="files(id, name, mimeType)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="allDrives",
        pageSize=1000,
    ).execute()
    children = res.get("files", [])
    target_norm = _normalize_name(name)

    # exact (after normalization)
    for f in children:
        if _normalize_name(f["name"]) == target_norm:
            return f

    # try with/without .txt
    tn_no = target_norm.removesuffix(".txt")
    for f in children:
        fn = _normalize_name(f["name"])
        if fn == tn_no or fn.removesuffix(".txt") == tn_no:
            return f

    # suggest close matches
    suggestions = difflib.get_close_matches(name, [f["name"] for f in children], n=5, cutoff=0.6)
    hint = f" Close matches: {suggestions}" if suggestions else " No similar names in folder."
    raise FileNotFoundError(f"File '{name}' not found in folder {folder_id}.{hint}")

# -------- download / upload helpers (Shared Drives aware) --------
def download_file_by_name_from_folder(drive, folder_id: str, name: str) -> str:
    file = _find_file_in_folder_by_name(drive, folder_id, name)
    if file["mimeType"] == "application/vnd.google-apps.document":
        request = drive.files().export_media(fileId=file["id"], mimeType="text/plain")
    else:
        request = drive.files().get_media(fileId=file["id"], supportsAllDrives=True)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue().decode("utf-8", errors="replace")

def download_csv_by_file_id(drive, file_id: str) -> List[Dict[str, str]]:
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    text = buf.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return [row for row in reader]

def upload_bytes_to_folder_as_file(drive, parent_folder_id: str, filename: str, mime_type: str, data: bytes):
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, resumable=False)
    file_metadata = {"name": filename, "parents": [parent_folder_id]}
    created = (
        drive.files()
        .create(body=file_metadata, media_body=media, fields="id, name", supportsAllDrives=True)
        .execute()
    )
    return created
