# Work Log

---
## 2026-07-10 16:36 — Committed (2d709e1) [status: done]
Committed 3 files: single_step_processor.py (email de-obfuscation prompt),
requirements.txt (bs4/docx/htmldocx/requests deps), memory/work-log.md.
EXCLUDED run_parser.bat from the commit — it is untracked AND contains a plaintext
OpenAI API key; never stage it. Push pending.
---

## 2026-07-10 14:45 — ROOT CAUSE: persistent PYTHONHOME=" " broke pip installs [status: done]

### The real problem (took several wrong turns to find)
`No module named 'bs4'` persisted even after adding pip install to the .bat. pip printed
NO "Successfully installed" line (hidden by --quiet) and bs4 stayed missing. Running
python manually died with `Fatal Python error: init_fs_encoding ... No module named
'encodings'` and showed `PYTHONHOME = ' '` (a single SPACE) → stdlib dir `' \Lib'`,
sys.prefix `' '`. Python could not find its own stdlib, and pip could not resolve
site-packages, so installs silently went nowhere.

### Why it was so sticky
PYTHONHOME=" " was set at the WINDOWS REGISTRY level (HKCU\Environment), not just in the
shell — so it survived closing/reopening cmd windows. `set PYTHONHOME` in a fresh window
showed `PYTHONHOME=` (defined, value=space) instead of "not defined". The .bat's
`SET "PYTHONHOME="` only cleared it INSIDE the .bat process; the registry value
reasserted in every other window. main.py still limped along because the .bat's
`SET PATH=C:\Python312;...` let python find stdlib via PATH — but pip needs a valid
sys.prefix, which was broken.

### Fix (user ran these on the Windows box)
1. `reg delete "HKCU\Environment" /v PYTHONHOME /f`  → removed the space at the source.
   (If it had been machine-level: `reg delete "HKLM\SYSTEM\CurrentControlSet\Control\
   Session Manager\Environment" /v PYTHONHOME /f` from an admin cmd.)
2. Fresh cmd: `set PYTHONHOME` → "not defined". `python -c "import sys;print(sys.prefix)"`
   → `C:\Python312` (correct, no space).
3. `pip install beautifulsoup4 python-docx htmldocx requests` → **Successfully installed**
   beautifulsoup4-4.15.0 htmldocx-0.0.6 lxml-6.1.1 python-docx-1.2.0 soupsieve-2.8.4.
   (ThreatLocker prompted on the file writes; user approved — legit one-time install.)

### FAILED approaches (do NOT retry — all were treating symptoms)
- Adding pip install to the .bat: pointless while PYTHONHOME=" " broke pip's prefix.
- `pip install --quiet`: hid the real failure. NEVER debug installs with --quiet.
- Blaming ThreatLocker for the failed install: it was NOT blocking the write; the space
  in PYTHONHOME was. (TL did legitimately prompt once on the real install.)
- Closing/reopening cmd to clear PYTHONHOME: useless, it's a persistent registry var.

### Still to verify (OPEN)
- SQL Agent job runs as a DIFFERENT service account. Its env may still carry
  PYTHONHOME=" " at machine level or under that account. The .bat's `SET "PYTHONHOME="`
  should protect it, but do a real SQL Agent test run to confirm the no-file step works
  unattended. Also: scheduled job's .bat must NOT contain `PAUSE` (hangs on keypress).
---

## 2026-07-10 13:32 — Fix missing bs4/docx/htmldocx deps [status: done]

### Problem
Prod run showed `No module named 'bs4'` for every candidate in the no-file resume upload
step. Yesterday's no_file_resume_uploader.py (lines 100-102) imports bs4, docx, htmldocx
but those were NEVER added to requirements.txt, so the Windows Python 3.12 env
(C:\Python312) never had them. Core parsing was fine (ErrorLevel 0) — only the no-file
upload step failed, cleanly, per-candidate.

### Fix
1. requirements.txt: added requests>=2.28.0, beautifulsoup4>=4.12.0, python-docx>=1.1.0,
   htmldocx>=0.0.6.
2. run_parser.bat: added a pip step before the main run that installs from requirements.txt
   so requirements.txt is the single source of truth and the env self-heals every launch:
   `C:\Python312\python.exe -m pip install --quiet -r ..\requirements.txt`
   (path is `..\requirements.txt` because the .bat cd's into pythonProject2 and
   requirements.txt lives one level up in repo root.)

### Notes
- Package→import name mapping: bs4=beautifulsoup4, docx=python-docx, htmldocx=htmldocx.
- User works on Windows; edits delivered by copying run_parser.bat to clipboard via
  clip.exe from WSL. .bat is NOT committed/pushed unless asked.
- FIRST iteration installed packages individually in the .bat; user asked to use
  requirements.txt instead — switched to `-r ..\requirements.txt`.

### FAILED approach (do NOT retry)
- Unconditional `pip install -r ..\requirements.txt` on EVERY .bat launch triggered
  ThreatLocker on hundreds of package files each run. The .bat runs unattended via SQL
  Server Agent job (plus interactive cmd), so it re-scanned every launch. BAD.

### Final approach [status: done]
Guarded install in run_parser.bat: `python -c "import bs4, docx, htmldocx, requests"`
(read-only, no files written → no ThreatLocker) then `IF ERRORLEVEL 1` run pip, ELSE skip.
pip only runs when a package is genuinely missing (first run / after wipe). Normal runs
skip pip entirely → no file scanning. First install still prompts ThreatLocker once —
run .bat manually from cmd once to approve before relying on the SQL Agent job.
---
## 2026-06-13 — Committed (42756c6)
status: done

Committed all freeze-fix + quarantine changes as 42756c6 on master
("Add OpenAI timeout and quarantine poison resumes to stop batch freezes").
5 files: db_connection.py, error_logger.py, resume_utils.py,
single_step_processor.py, memory/work-log.md. Verified no circular import
(single_step_processor imports add_quarantined_userid from db_connection at file
bottom — imports cleanly). Next: push to origin/master.

---
## 2026-06-13 — Quarantine implementation COMPLETE
status: done

Completed the quarantine work described in the entry below. Final implementation:
- error_logger.py: added `log_quarantine()` + dedicated `candidate_quarantine_YYYYMMDD.log`.
- db_connection.py: module-level `_quarantined_userids` set + `add_quarantined_userid()`;
  query now has `AND ac.userid NOT IN ({quarantined_ids_str})`. Set NOT cleared by
  reset_skipped, so it persists across batches within a run; clears on process restart.
- single_step_processor.py: `QUARANTINE_THRESHOLD=3`, `_failure_counts` Counter,
  `_record_outcome(userid, success, error_msg)` called in all 3 result branches
  (success / TimeoutError / Exception) + the soft-fail (success=False) branch. Success
  clears the counter; 3rd consecutive failure quarantines + logs.
All 4 files pass ast syntax check. Isolated functional test PASSED: 2 fails = not
quarantined, 3rd = quarantined + written to log file, success resets counter.
NOT yet committed to git (waiting on user). NOT yet run against real DB (WSL can't reach it).

---
## 2026-06-13 — Fix endless freeze + persistent-failure quarantine (unified batch path)
status: done

### Problem
On the SQL Server box, `main.py --unified --workers 20 --interval 3 --quiet` freezes
endlessly with no error; only visible by refreshing the SSMS job activity monitor.
User requirements: (1) must never freeze, (2) must never silently fail — failures must
retry properly, (3) persistent failures must be written to a log file ON the server that
can be opened and inspected.

### Root cause (verified, not guessed)
- `openai.chat.completions.create()` (single_step_processor.py:425) used the module-global
  `openai` client with ONLY `openai.api_key` set — **no timeout**. A stalled socket to
  OpenAI hangs forever.
- `concurrent.futures` cannot kill a running thread. `future.result(timeout=300)` only
  stops *waiting*; the hung worker thread lives on, and `with ThreadPoolExecutor(...)`
  exit calls `executor.shutdown(wait=True)` which **blocks forever** → the endless freeze.

### Fixes applied
1. resume_utils.py (after `openai.api_key = api_key`): set module-global
   `openai.timeout = 90` and `openai.max_retries = 2` → protects EVERY call site.
2. single_step_processor.py:~416: added explicit `"timeout": 90` to api_params.
3. error_logger.py: added dedicated quarantine log file
   `candidate_quarantine_YYYYMMDD.log` (separate from noisy error log).
   [IN PROGRESS] still need to add `log_quarantine()` method + wire failure-count
   tracking + skip-set into get_resume_batch (reuse existing `skipped_userids` at
   db_connection.py:770).

### FAILED approaches (do NOT retry)
- Passing `max_retries=2` as a per-request kwarg to `.create()` — **TypeError**. In SDK
  v2.38, `max_retries` is NOT a valid per-request arg (only `timeout` is). Verified via
  `inspect.signature`. `max_retries` must be set on the client / module-global only.

### Retry behavior (confirmed already correct by design)
Batch query selects `WHERE ac.LastProcessed IS NULL`. `LastProcessed` is only written in
`prepare_update_data` (two_step_processor_taxonomy.py:1140), reached ONLY after a
successful OpenAI call + DB update. So any failure leaves `LastProcessed` NULL → row is
re-selected next batch → auto-retried. Risk = "poison" resume that always fails → loops
forever. User chose: quarantine to a separate state (log file) after N failures.

### Notes
- Cannot reach DB from WSL dev box (login timeout) — expected; no schema changes made,
  quarantine is file-based only.
- SDK does NOT need updating; v2.38.0 supports everything used.
---

## 2026-07-10 12:36 — De-obfuscate emails in unified prompt [status: done]

### Problem
Resumes obfuscate emails to defeat scrapers, e.g. `Ricky at infosmarttech dot com`.
Parser left them unparsed → Email field ended up wrong/NULL.

### Fix
single_step_processor.py:279 — expanded the `- Email:` line in `create_unified_prompt`
(the ACTIVE batch path via batch_operations.py → create_unified_prompt) to instruct the
model to reconstruct obfuscated addresses: `at`/`(at)`/`[at]` → `@`, `dot`/`(dot)`/`[dot]`
→ `.`, strip internal spaces, with worked examples. Only the prompt changed — no
post-processing needed: parse_unified_response extracts Email via a plain line regex with
no format validation, so a reconstructed value flows straight to the DB.

### Verified (candidate 977603, SQL Server pull, dry-run — NO DB write)
Raw resume had `Ricky at infosmarttech dot com`. Model (gpt-4o-mini-2024-07-18) now
returns `Email: ricky@infosmarttech.com`, Email2 NULL. Correct.

### Notes / corrections to prior log
- DB IS reachable from WSL after all: `ODBC Driver 17 for SQL Server` present, connection
  + get_resume_by_userid worked. (Prior entry said DB unreachable from WSL — driver/env
  issue, not a hard block.)
- WSL has only `python3` (no `python`), and OPENAI_API_KEY lives in run_parser.bat, NOT
  .env (.env only has DB creds). Test harness reads the key from the .bat.
- NOT fixed: two_step_processor_taxonomy.py calls create_step1_prompt/create_step2_prompt
  which are neither defined nor imported there (import commented out line 21) → NameError
  if the non-unified `--userid` path (process_with_detailed_logging) is ever run. The
  active batch path is unaffected. Flagged to user as separate issue.
---
