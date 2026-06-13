# Work Log

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
