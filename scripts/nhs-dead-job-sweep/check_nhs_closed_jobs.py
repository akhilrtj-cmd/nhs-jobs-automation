"""
Retires or removes NHS Jobs (jobs.nhs.uk) listings that the site now shows as
closed.

NHS closes applications at 23:59 on the closing date. The advert keeps its
URL, but its closing-date line changes from "The closing date is <date>" to
"This job is now closed". That wording is the positive evidence this sweep
looks for, and the workflow runs at 00:05 UK time, just after the day's
closures.

The NHS Jobs advert page is not always the whole story, though. Most adverts
route "Apply for this job" to a THIRD-PARTY applicant tracking system (Trac,
in every case confirmed live) via a two-click NHS Jobs handoff
(#apply-ats-direct, then a form submit on #ats-direct-apply-continue -- not a
plain link, so it has to be clicked through with a real browser, not just
fetched). The employer can close applications on that third-party system
independently, and the NHS Jobs advert page itself is NOT guaranteed to
update in step -- confirmed live: 1 of 5 randomly sampled "still open" NHS
adverts had already closed on its Trac destination while the NHS Jobs page
kept showing a future closing date with no "now closed" wording at all. A
job that looks ALIVE on the advert page is therefore followed through that
same handoff, and closed wording on the FINAL destination page overrides the
advert page's own verdict. A job that looks DEAD already (closed wording, or
404/410) skips this extra check -- it's redundant once the advert page
itself already gives positive evidence.

Safety rules, mirroring scripts/dead-job-sweep/check_dead_jobs.py:

* A job someone has applied to is retired (isActive = false, closedAt = now),
  never hard-deleted, because Application.job is onDelete: Cascade and
  deleting the job would destroy the application record.
* Only positive evidence counts as dead: closed wording, or HTTP 404/410.
  Timeouts, blocks and unexpected pages are UNKNOWN and left alone.
* If the run cannot verify pages in a row, it stops without changing anything.
* If a large share of checked pages look closed at once, --apply is refused.
  That pattern is more likely a layout change than a real mass closure.

Usage:
  python check_nhs_closed_jobs.py              # dry run: writes the report only
  python check_nhs_closed_jobs.py --apply      # retire / delete
  python check_nhs_closed_jobs.py --limit 20   # check the first 20 only
"""
import csv
import os
import sys
import time

import psycopg2
from playwright.sync_api import sync_playwright

REPORT_CSV = "nhs_dead_jobs_report.csv"
MAX_CLOSED_SHARE = 0.5
MIN_SAMPLE_FOR_CAP = 20
MAX_CONSECUTIVE_UNKNOWN = 15
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
CLOSED_PHRASES = ("this job is now closed", "this job is no longer available")
# Generic third-party ATS wording -- these pages are NOT NHS Jobs pages, so
# NHS Jobs's own "this job is now closed" phrasing doesn't apply here. Kept
# separate from CLOSED_PHRASES rather than merged, since a false match on
# the NHS Jobs page itself and a false match on an arbitrary third-party
# page carry different risk and are worth being able to tell apart in the
# report's "reason" column.
THIRD_PARTY_CLOSED_PHRASES = (
    "no longer accepting applications",
    "no longer being accepted",
    "vacancy is closed",
    "vacancy has closed",
    "vacancy has expired",
    "job has expired",
    "posting has expired",
    "position has been filled",
    "no longer available",
)
MAX_CONSECUTIVE_THIRD_PARTY_FAILURES = 10


def get_database_url():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("ABORT: DATABASE_URL is not set in the environment.")
        sys.exit(1)
    return url.strip()


def fetch(page, url):
    """Returns (status, html). status is None and html is None when every
    attempt failed, which classify() treats as UNKNOWN."""
    for attempt in range(3):
        try:
            resp = page.goto(url, wait_until="load", timeout=30000)
            page.wait_for_timeout(300)
            return (resp.status if resp else None), page.content()
        except Exception:
            time.sleep(3 * (attempt + 1))
    return None, None


def classify(status, html):
    if html is None:
        return "UNKNOWN", "page could not be loaded"
    low = html.lower()
    if status in (404, 410):
        return "DEAD", f"http {status}"
    if any(p in low for p in CLOSED_PHRASES) or "nhs-closed-job-inset" in low:
        return "DEAD", "page shows job is closed"
    if status and 200 <= status < 400 and "closing date is" in low:
        return "ALIVE", "closing date shown"
    return "UNKNOWN", f"http {status}, no closed or closing-date wording"


def check_third_party_destination(page):
    """Assumes `page` is currently on an NHS Jobs advert page that looked
    ALIVE. Clicks through the real "Apply for this job" handoff and checks
    the final destination for closure wording.

    Returns one of:
      ("closed", reason)   -- the third-party system shows this is closed;
                               overrides the advert page's own ALIVE verdict.
      ("open", reason)     -- reached a final page with no closure wording.
      ("not_applicable", reason) -- this advert has no #apply-ats-direct
                               handoff at all (applies within NHS Jobs
                               itself), so there's nothing further to check.
      ("unknown", reason)  -- the handoff exists but couldn't be completed
                               (missing continue button, navigation timeout,
                               etc.) -- left alone, never treated as closed.
    """
    try:
        apply_btn = page.query_selector("#apply-ats-direct")
        if apply_btn is None:
            return "not_applicable", "no third-party handoff on this advert"

        page.click("#apply-ats-direct")
        page.wait_for_load_state("load", timeout=20000)

        continue_btn = page.query_selector("#ats-direct-apply-continue")
        if continue_btn is None:
            return "unknown", "handoff page had no continue button"

        with page.expect_navigation(wait_until="load", timeout=20000):
            page.click("#ats-direct-apply-continue")
        page.wait_for_timeout(300)
        final_text = page.content().lower()

        for phrase in THIRD_PARTY_CLOSED_PHRASES:
            if phrase in final_text:
                return "closed", f"third-party destination ({page.url}) shows: {phrase}"
        return "open", f"third-party destination ({page.url}) shows no closure wording"
    except Exception as e:
        return "unknown", f"could not complete third-party handoff: {e}"


def write_report(results):
    with open(REPORT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["id", "title", "company", "applicationUrl", "applications", "verdict", "reason"]
        )
        writer.writeheader()
        writer.writerows(results)


def apply_changes(db_url, to_retire, to_delete):
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            retired = deleted = 0
            if to_retire:
                cur.execute(
                    'UPDATE "Job" SET "isActive" = false, "closedAt" = NOW() '
                    'WHERE id = ANY(%s) AND "isActive" = true',
                    (to_retire,),
                )
                retired = cur.rowcount
            if to_delete:
                cur.execute('DELETE FROM "Job" WHERE id = ANY(%s)', (to_delete,))
                deleted = cur.rowcount
        conn.commit()
        return retired, deleted
    finally:
        conn.close()


def main():
    do_apply = "--apply" in sys.argv
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None

    db_url = get_database_url()
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT j.id, j.title, j.company, j."applicationUrl",
               (SELECT COUNT(*) FROM "Application" a WHERE a."jobId" = j.id)
        FROM "Job" j
        WHERE j."applicationUrl" LIKE %s AND j."isActive" = true
        ORDER BY j.id
        """,
        ("%jobs.nhs.uk%",),
    )
    jobs = cur.fetchall()
    cur.close()
    conn.close()
    if limit:
        jobs = jobs[:limit]

    print(f"Checking {len(jobs)} live NHS listing(s) | apply={do_apply}", flush=True)

    results = []
    aborted = False
    consecutive_unknown = 0
    consecutive_third_party_failures = 0
    third_party_checks_disabled = False
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ("image", "media", "font")
            else route.continue_(),
        )
        page = context.new_page()

        for i, (job_id, title, company, url, app_count) in enumerate(jobs, 1):
            status, html = fetch(page, url)
            verdict, reason = classify(status, html)

            # Only worth following through for a job that still looks ALIVE
            # from the advert page alone -- a DEAD/UNKNOWN verdict here is
            # either already correct or already unverifiable, and the page
            # we'd need to click through from (the one `page` is currently
            # on) only reflects the advert page `fetch()` just loaded when
            # that load actually succeeded.
            if verdict == "ALIVE" and not third_party_checks_disabled:
                tp_status, tp_reason = check_third_party_destination(page)
                if tp_status == "closed":
                    verdict, reason = "DEAD", tp_reason
                    consecutive_third_party_failures = 0
                elif tp_status == "unknown":
                    consecutive_third_party_failures += 1
                    if consecutive_third_party_failures >= MAX_CONSECUTIVE_THIRD_PARTY_FAILURES:
                        third_party_checks_disabled = True
                        print(f"    (disabling third-party checks for the rest of this run after "
                              f"{consecutive_third_party_failures} in a row couldn't be completed)", flush=True)
                else:
                    consecutive_third_party_failures = 0

            print(f"[{i}/{len(jobs)}] {verdict:7} {title} @ {company} ({reason})", flush=True)
            results.append({
                "id": job_id,
                "title": title,
                "company": company,
                "applicationUrl": url,
                "applications": app_count,
                "verdict": verdict,
                "reason": reason,
            })

            consecutive_unknown = consecutive_unknown + 1 if verdict == "UNKNOWN" else 0
            if consecutive_unknown >= MAX_CONSECUTIVE_UNKNOWN:
                print(f"ABORT: {consecutive_unknown} pages in a row could not be verified. "
                      "The site may be blocking or down. Nothing will be changed.", flush=True)
                aborted = True
                break

        browser.close()

    write_report(results)

    dead = [r for r in results if r["verdict"] == "DEAD"]
    to_retire = [r["id"] for r in dead if r["applications"] > 0]
    to_delete = [r["id"] for r in dead if r["applications"] == 0]
    alive = sum(1 for r in results if r["verdict"] == "ALIVE")
    unknown = sum(1 for r in results if r["verdict"] == "UNKNOWN")

    print()
    print(f"Checked: {len(results)} | alive: {alive} | closed: {len(dead)} | unverified: {unknown}")
    print(f"Closed with applications (retire): {len(to_retire)}")
    print(f"Closed with no applications (delete): {len(to_delete)}")
    print(f"Report written to {REPORT_CSV}")

    if not do_apply:
        print("Dry run -- nothing changed. Pass --apply to write.")
        return
    if aborted:
        print("Not applying: the run was aborted.")
        return
    if len(results) >= MIN_SAMPLE_FOR_CAP and len(dead) / len(results) > MAX_CLOSED_SHARE:
        print(f"Not applying: {len(dead)} of {len(results)} pages look closed at once. "
              "That is more likely a layout change than a real mass closure. Check the report.")
        return
    if not dead:
        print("Nothing to change.")
        return

    retired, deleted = apply_changes(db_url, to_retire, to_delete)
    print(f"Applied: retired {retired}, deleted {deleted}.")


if __name__ == "__main__":
    main()
