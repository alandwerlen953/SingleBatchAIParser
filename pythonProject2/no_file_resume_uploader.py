"""Pipeline step: give no-file profiles a resume doc so they get parsed.

Candidates whose resume was pasted into the Bullhorn profile (text in
Candidate.description) but who have NO file attachment never get ingested
into aicandidate, so the parser never sees them. This step finds recent
candidates in that state, converts their description HTML to a .docx, and
uploads it to Bullhorn as a Resume attachment — after which the normal
file -> ingest -> markdown -> parse pipeline handles them.

Runs at the start of every batch cycle (see main.py). Idempotent: a
candidate that already has a live file is skipped, so re-runs are safe.

Window: Candidate.dateAdded within the last 3 months — same horizon the
parser's own batch query uses (lastprocessedmarkdown >= 3 months back).

Requires Bullhorn credentials in env/.env (BULLHORN_CLIENT_ID,
BULLHORN_CLIENT_SECRET, BULLHORN_USERNAME, BULLHORN_PASSWORD) and packages:
python-docx, htmldocx, beautifulsoup4.

Standalone test:
    python no_file_resume_uploader.py            # full step, real uploads
    python no_file_resume_uploader.py --dry-run  # find + convert only
"""
import os
import io
import re
import time
import logging
import argparse

from db_connection import create_pyodbc_connection
from bullhorn_sync_client import BullhornClient

logger = logging.getLogger(__name__)

BH_DELAY = 0.020          # 50 req/sec max — stay well under
BULK_CHUNK = 100          # candidate ids per multi-id file check
MIN_DESCRIPTION_LEN = 50  # ignore stub descriptions
WINDOW_MONTHS = 3         # same recency horizon as the parse batch query
DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
AUDIT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "generated_resume_docs")


def _get_candidates_missing_from_pipeline():
    """Recent candidates with description text but no aicandidate row."""
    conn, ok, msg = create_pyodbc_connection()
    if not ok:
        raise RuntimeError(f"SQL connection failed: {msg}")
    query = f"""
        SELECT c.candidateID, c.name, c.description
        FROM dbo.Candidate c WITH (NOLOCK)
        LEFT JOIN dbo.aicandidate ac WITH (NOLOCK) ON ac.userid = c.candidateID
        WHERE c.dateAdded >= DATEADD(month, -{WINDOW_MONTHS}, GETDATE())
          AND c.isDeleted = 0
          AND c.description IS NOT NULL
          AND LEN(c.description) > {MIN_DESCRIPTION_LEN}
          AND ac.userid IS NULL
        ORDER BY c.dateAdded
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows


def _bulk_no_file_ids(bh, ids):
    """Subset of ids that have zero live file attachments in Bullhorn."""
    no_files = []
    for start in range(0, len(ids), BULK_CHUNK):
        chunk = ids[start:start + BULK_CHUNK]
        bh._ensure_session()
        url = f"{bh.rest_url}entity/Candidate/{','.join(str(i) for i in chunk)}"
        params = {"BhRestToken": bh.bh_rest_token,
                  "fields": "id,fileAttachments(id,isDeleted)"}
        resp = bh.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if isinstance(data, dict):
            data = [data]
        for cand in data:
            fa = cand.get("fileAttachments") or {}
            total = fa.get("total", 0)
            files = fa.get("data", [])
            if total == 0:
                no_files.append(cand["id"])
            elif (not any(not f.get("isDeleted") for f in files)
                  and len(files) >= total):
                no_files.append(cand["id"])  # all attachments visible + deleted
            # anything with a live (or possibly-live) file is left alone
        time.sleep(BH_DELAY)
    return no_files


def _html_to_docx_bytes(html):
    """Convert description HTML to docx, working around htmldocx crashes:
    <a> without href (KeyError), block-level <br> (no open paragraph),
    irregular tables (grid IndexError) — all seen in real pasted resumes."""
    from bs4 import BeautifulSoup
    from docx import Document
    from htmldocx import HtmlToDocx

    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a"):
        if not a.get("href"):
            a.unwrap()
    for br in soup.find_all("br"):
        if br.parent and br.parent.name in ("div", "body", "[document]"):
            br.replace_with(soup.new_tag("p"))
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        cell_counts = {len(r.find_all(["td", "th"])) for r in rows}
        has_span = any(c.get("colspan") or c.get("rowspan")
                       for c in table.find_all(["td", "th"]))
        if len(cell_counts) > 1 or has_span or not rows:
            for r in rows:
                p = soup.new_tag("p")
                p.string = " | ".join(
                    c.get_text(" ", strip=True) for c in r.find_all(["td", "th"])
                    if c.get_text(strip=True))
                table.insert_before(p)
            table.decompose()

    doc = Document()
    HtmlToDocx().add_html_to_document(str(soup), doc)
    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


def _upload_resume(bh, candidate_id, filename, docx_bytes):
    bh._ensure_session()
    url = f"{bh.rest_url}file/Candidate/{candidate_id}/raw"
    params = {"BhRestToken": bh.bh_rest_token,
              "externalID": "pastedResume",
              "fileType": "SAMPLE",
              "type": "Resume"}
    resp = bh.session.put(url, params=params,
                          files={"file": (filename, docx_bytes, DOCX_MIME)},
                          timeout=60)
    resp.raise_for_status()
    return resp.json().get("fileId")


def run_no_file_backfill(dry_run=False):
    """Find recent no-file profiles with pasted resume text and upload a
    generated .docx for each. Returns dict of counts."""
    counts = {"candidates_checked": 0, "no_file": 0, "uploaded": 0, "error": 0}

    rows = _get_candidates_missing_from_pipeline()
    counts["candidates_checked"] = len(rows)
    logger.info(f"[no-file step] {len(rows)} recent candidates with pasted "
                f"resume text and no aicandidate row")
    if not rows:
        return counts

    by_id = {r[0]: r for r in rows}
    bh = BullhornClient()
    bh.login()

    no_file_ids = _bulk_no_file_ids(bh, list(by_id))
    counts["no_file"] = len(no_file_ids)
    logger.info(f"[no-file step] {len(no_file_ids)} of them have zero live "
                f"files in Bullhorn")

    for cid in no_file_ids:
        _, name, description = by_id[cid]
        try:
            docx_bytes = _html_to_docx_bytes(description)
            safe_name = re.sub(r"[^A-Za-z0-9 _-]", "", name or "").strip() or str(cid)
            filename = f"{safe_name} Resume.docx"

            os.makedirs(AUDIT_DIR, exist_ok=True)
            with open(os.path.join(AUDIT_DIR, f"{cid} - {filename}"), "wb") as f:
                f.write(docx_bytes)

            if dry_run:
                logger.info(f"[no-file step] DRY RUN {cid} ({name}): would "
                            f"upload '{filename}' ({len(docx_bytes)} bytes)")
                continue

            file_id = _upload_resume(bh, cid, filename, docx_bytes)
            counts["uploaded"] += 1
            logger.info(f"[no-file step] {cid} ({name}): uploaded "
                        f"'{filename}' fileId={file_id}")
        except Exception as e:
            counts["error"] += 1
            logger.error(f"[no-file step] {cid} ({name}): {e}")
        time.sleep(BH_DELAY)

    logger.info(f"[no-file step] done: {counts}")
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="Find and convert, but do not upload")
    args = ap.parse_args()
    print(run_no_file_backfill(dry_run=args.dry_run))
