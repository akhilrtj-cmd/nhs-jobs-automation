"""
Unattended weekly pipeline: scrapes every current NHS Jobs Band 3 and Band 4
vacancy, classifies Skilled Worker visa sponsorship eligibility, and imports
the jobs that pass a STRICT eligibility gate straight into the live database.
No human reviews this before it goes live -- see "Why a stricter gate than
the manual pipeline" below for what that changes.

This is a self-contained port of the classifier logic from the manually-run
pipeline at `C:\\Users\\akhil\\site search\\extract_nhs_jobs.py` (a separate,
non-git local folder this workflow's GitHub runner has no access to). Keep
the NEGATIVE_PATTERNS / POSITIVE_PATTERNS / find_unrecognised_negative_sentence
logic here in sync with that file if either one gets a new fix -- they are
two copies of the same hard-won rules, not two independent implementations.

Why a stricter gate than the manual pipeline
----------------------------------------------
The manually-run pipeline accepts a positive sponsorship match found
anywhere on the page when there's no dedicated "Certificate of Sponsorship"
section (full-page fallback) -- acceptable there because a human looks at
every row in the admin queue before it goes live. Nobody reviews this
pipeline's output before publishing, so STRICT_eligible() below requires
MORE than classify_sponsorship()'s own "eligible=True":
  1. classify_sponsorship() must say eligible=True (no negative match
     anywhere on the page, no unrecognised-but-suspicious sentence).
  2. A dedicated Certificate of Sponsorship section must exist AND the
     positive phrase must be found WITHIN that section specifically --
     the full-page fallback is refused here, even though the manual
     pipeline accepts it.
  3. No stated contract Duration of MIN_DURATION_MONTHS or less.
  4. No "see attached ... for full details" style pointer to a PDF the
     scraper never reads -- an incomplete description is a quality risk
     distinct from sponsorship eligibility, but still not something that
     should auto-publish unreviewed.
Every candidate job, whatever the outcome, is written to the audit CSV this
script uploads as a workflow artifact -- the closest thing to a review this
pipeline gets, after the fact rather than before.

Safety caps, on top of the strict gate itself:
  * Dedup against the live DB by applicationUrl before importing -- a job
    already live from a previous run is never reinserted.
  * If the search returns suspiciously few results for a pay band (the site
    layout may have changed, or a request may be getting blocked), the run
    aborts without importing anything rather than publish from bad data.
    Each band has its own expected-results floor in MIN_EXPECTED_RESULTS,
    since Band 3 and Band 4 run at very different volumes.
  * If more than MAX_IMPORT_PER_RUN jobs would be imported in one run, the
    run aborts without importing -- more likely a dedup/scrape bug than a
    real surge, between runs only ~2-3 days apart.

Usage:
  python extract_classify_import.py              # classify + import
  python extract_classify_import.py --dry-run    # classify only, no DB writes
  python extract_classify_import.py --limit 20   # testing only -- the production
                                                   # workflow never passes this; a
                                                   # real run always covers all of
                                                   # Band 3 and Band 4, since there's
                                                   # no persisted "already seen" state
                                                   # between separate scheduled runs
                                                   # to resume from.
"""
import csv
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from urllib.parse import urlencode

import psycopg2
from lxml import html
from playwright.sync_api import sync_playwright

SEARCH_KEYWORD = ""
# The site 400s on a request naming more than one payBand value, so each
# band is searched in its own separate pass (see the scraping loop in main()).
PAY_BANDS = ["BAND_3", "BAND_4"]
BASE_URL = "https://www.jobs.nhs.uk"
SEARCH_PATH = "/candidate/search/results"
REPORT_CSV = "nhs_band3_auto_import_report.csv"

MIN_DURATION_MONTHS = 8
# Band 3 has consistently reported 700-850 results; Band 4 runs at roughly
# half that volume. Well below either floor is suspicious (site layout
# changed, or the request is being blocked) and aborts that band's run.
MIN_EXPECTED_RESULTS = {"BAND_3": 200, "BAND_4": 100}
# Raised from the Band-3-only 250 to give headroom for a combined
# Band 3 + Band 4 run without needing a second, separate cap.
MAX_IMPORT_PER_RUN = 300

HCSW_SOC_CODE = "6131"


def infer_soc_code(title):
    if "healthcare support worker" in title.lower():
        return HCSW_SOC_CODE
    return ""


def parse_duration_months(text):
    text = text.strip().lower()
    m = re.search(r"([\d.]+)\s*(week|month|year)", text)
    if not m:
        return None
    n = float(m.group(1))
    unit = m.group(2)
    if unit == "week":
        return n / 4.345
    if unit == "year":
        return n * 12
    return n


# --- Sponsorship classifier (kept in sync with the manual pipeline's copy) -
NEGATIVE_PATTERNS = [
    re.compile(r"not\s+(?:be\s+)?eligible\s+for\s+.{0,40}?sponsorship", re.I),
    re.compile(r"does\s+not\s+meet\s+the\s+(?:ukvi\s+)?eligibility\s+(?:criteria|requirements)\s+for\s+(?:a\s+)?(?:skilled\s+worker\s+)?(?:visa|sponsorship)", re.I),
    re.compile(r"unable\s+to\s+(?:offer|provide)\s+.{0,40}?sponsorship", re.I),
    re.compile(r"(?:does|do)\s+not\s+offer\s+(?:visa\s+)?sponsorship", re.I),
    re.compile(r"does\s+not\s+attract\s+sponsorship", re.I),
    re.compile(r"we\s+are\s+unable\s+to\s+sponsor", re.I),
    re.compile(r"sponsorship\s+is\s+not\s+available", re.I),
    re.compile(r"not\s+able\s+to\s+sponsor", re.I),
    re.compile(r"not\s+be\s+able\s+to\s+issue\s+a\s+certificate\s+of\s+sponsorship", re.I),
    re.compile(r"cannot\s+accept.{0,60}?requiring\s+sponsorship", re.I | re.S),
]
WITHDRAWN_PHRASE = re.compile(r"your\s+application\s+(?:may|will)\s+be\s+withdrawn", re.I)
WITHDRAWN_CONTEXT = re.compile(r"sponsor|visa", re.I)
WITHDRAWN_CONTEXT_WINDOW = 200
POSITIVE_PATTERNS = [
    re.compile(r"welcome\s+and\s+will\s+be\s+considered", re.I),
    re.compile(r"require\s+current\s+skilled\s+worker\s+sponsorship.{0,40}(?:welcome|considered)", re.I | re.S),
]
SPONSOR_WORD_RE = re.compile(r"sponsor", re.I)
NEGATION_WORD_RE = re.compile(r"\bnot\b|\bno\b|n't\b|cannot|unable|without|ineligible|excluded", re.I)
UNRECOGNISED_WINDOW = 100

PDF_POINTER_RE = re.compile(
    "|".join([
        r"please\s+see\s+(?:the\s+)?attached",
        r"please\s+refer\s+to\s+(?:the\s+)?attached",
        r"attached\s+(?:job\s+)?(?:description|document|pdf|pack)",
        r"see\s+attached\s+(?:detailed\s+)?(?:job\s+description|document|jd|person\s+spec)",
        r"attached\s+job\s+description\s+and\s+person\s+specification",
        r"applicant\s+information\s+pack",
        r"for\s+(?:further|more)\s+(?:details|information)\s*,?\s*please\s+see",
        r"supporting\s+document",
        r"further\s+information\s+can\s+be\s+found\s+in\s+the\s+attached",
        r"please\s+view\s+the\s+attached",
    ]),
    re.I,
)


def find_unrecognised_negative_sentence(full_text):
    for m in SPONSOR_WORD_RE.finditer(full_text):
        start = max(0, m.start() - UNRECOGNISED_WINDOW)
        end = min(len(full_text), m.end() + UNRECOGNISED_WINDOW)
        window = full_text[start:end]
        if NEGATION_WORD_RE.search(window):
            return window.strip()[:300]
    return None


def classify_sponsorship(dedicated_text, full_text):
    for pat in NEGATIVE_PATTERNS:
        m = pat.search(full_text)
        if m:
            start = max(0, m.start() - 80)
            end = min(len(full_text), m.end() + 80)
            return False, full_text[start:end].strip(), False

    m = WITHDRAWN_PHRASE.search(full_text)
    if m:
        ctx_start = max(0, m.start() - WITHDRAWN_CONTEXT_WINDOW)
        ctx_end = min(len(full_text), m.end() + WITHDRAWN_CONTEXT_WINDOW)
        if WITHDRAWN_CONTEXT.search(full_text[ctx_start:ctx_end]):
            start = max(0, m.start() - 80)
            end = min(len(full_text), m.end() + 80)
            return False, full_text[start:end].strip(), False

    unrecognised = find_unrecognised_negative_sentence(full_text)
    if unrecognised:
        return None, unrecognised, True

    search_scope = dedicated_text if dedicated_text else full_text
    for pat in POSITIVE_PATTERNS:
        m = pat.search(search_scope)
        if m:
            start = max(0, m.start() - 80)
            end = min(len(search_scope), m.end() + 80)
            return True, search_scope[start:end].strip(), False

    return None, "", True


def strict_auto_import_check(job):
    """The extra, independent gate this unattended pipeline applies on top
    of classify_sponsorship()'s own verdict. Returns (passes: bool, reason).
    """
    if job["sponsorship_eligible"] is not True:
        return False, "classifier did not return eligible=True"

    dedicated = job["sponsorship_section_text"]
    if not dedicated:
        return False, "no dedicated Certificate of Sponsorship section on this advert"
    if not any(pat.search(dedicated) for pat in POSITIVE_PATTERNS):
        return False, "positive match was outside the dedicated section (full-page fallback) -- refused for auto-import"

    dm = job["duration_months"]
    if dm is not None and dm <= MIN_DURATION_MONTHS:
        return False, f"fixed-term, {dm:.1f} months (<= {MIN_DURATION_MONTHS})"

    if PDF_POINTER_RE.search(job["description"]):
        return False, "description points to an attached PDF for full details"

    return True, "passed strict gate"


# --- Scraping ----------------------------------------------------------------
def search_results_url(page_num, pay_band):
    params = {"keyword": SEARCH_KEYWORD, "payBand": pay_band, "language": "en", "page": page_num}
    return f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"


def parse_search_results(page_html):
    tree = html.fromstring(page_html)
    results = []
    for a in tree.xpath('//a[@data-test="search-result-job-title"]'):
        href = a.get("href", "")
        m = re.search(r"/candidate/jobadvert/([^?]+)", href)
        if m:
            results.append(m.group(1))
    return results


def get_total_result_count(page_html):
    m = re.search(r'aria-label="(\d+)\s+jobs?\s+found', page_html)
    return int(m.group(1)) if m else None


def goto_with_retry(page, url, attempts=4, base_delay=3):
    last_err = None
    for attempt in range(attempts):
        try:
            return page.goto(url, wait_until="load", timeout=30000)
        except Exception as e:
            last_err = e
            if attempt < attempts - 1:
                time.sleep(base_delay * (attempt + 1))
    raise last_err


def text_or_empty(tree, xpath_expr):
    els = tree.xpath(xpath_expr)
    return els[0].text_content().strip() if els else ""


def parse_job_detail(ref, page_html):
    tree = html.fromstring(page_html)
    title = text_or_empty(tree, '//h1[@id="heading"]')
    employer = text_or_empty(tree, '//span[@id="employer_name"]')
    band = text_or_empty(tree, '//p[@id="payscheme-band"]')
    salary = re.sub(r"\s+", " ", text_or_empty(tree, '//p[@id="range_salary"]'))
    job_type_nodes = tree.xpath('//h3[@id="working_pattern_heading"]/following-sibling::p[1]')
    job_type = re.sub(r"\s+", " ", job_type_nodes[0].text_content()).strip(" ,") if job_type_nodes else ""
    town = text_or_empty(tree, '//p[@id="employer_town"]')
    postcode = text_or_empty(tree, '//p[@id="employer_postcode"]')
    location = ", ".join(x for x in [town, postcode] if x)

    sections = []
    for heading_text, elem_id in [
        ("Job summary", "job_overview"), ("Main duties of the job", "job_description"), ("About us", "about_organisation"),
    ]:
        nodes = tree.xpath(f'//p[@id="{elem_id}"]')
        if not nodes:
            continue
        body_parts = []
        for el in nodes[0].itersiblings():
            if el.tag in ("h2", "h3", "div", "details"):
                break
            body_parts.append(html.tostring(el, encoding="unicode"))
        body_html = "".join(body_parts)
        if body_html.strip():
            sections.append(f"<h3>{heading_text}</h3>{body_html}")

    for div in tree.xpath('//div[contains(concat(" ", normalize-space(@class), " "), " hide-mobile ")]'):
        heading_nodes = div.xpath(".//h2")
        heading_text = heading_nodes[0].text_content().strip() if heading_nodes else ""
        if heading_text.lower() == "employer details":
            continue
        body_html = "".join(html.tostring(child, encoding="unicode") for child in div)
        if body_html.strip():
            sections.append(body_html)

    description = "\n".join(sections)

    duration_match = re.search(r'id="contract_duration">([^<]+)</p>', description, re.I)
    duration_months = parse_duration_months(duration_match.group(1)) if duration_match else None

    sponsorship_nodes = tree.xpath('//h3[@id="tier-two-sponsorship"]/following-sibling::p[1]')
    dedicated_text = sponsorship_nodes[0].text_content().strip() if sponsorship_nodes else ""

    full_text_nodes = tree.xpath('//main[@id="maincontent"]')
    full_text = full_text_nodes[0].text_content() if full_text_nodes else tree.text_content()
    full_text = re.sub(r"\s+", " ", full_text)

    eligible, match_text, needs_review = classify_sponsorship(dedicated_text, full_text)

    return {
        "job_reference": ref,
        "title": title,
        "employer_trust": employer,
        "location": location,
        "salary": salary,
        "band": band,
        "job_type": job_type,
        "soc_code": infer_soc_code(title),
        "applicationUrl": f"{BASE_URL}/candidate/jobadvert/{ref}",
        "description": description,
        "duration_months": duration_months,
        "sponsorship_section_text": dedicated_text,
        "sponsorship_match_text": match_text,
        "sponsorship_eligible": eligible,
    }


# --- DB ------------------------------------------------------------------
def get_database_url():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("ABORT: DATABASE_URL is not set in the environment.")
        sys.exit(1)
    return url.strip()


def get_live_nhs_urls(db_url):
    conn = psycopg2.connect(db_url)
    try:
        cur = conn.cursor()
        cur.execute('SELECT "applicationUrl" FROM "Job" WHERE "applicationUrl" LIKE %s', ("%jobs.nhs.uk%",))
        return set(r[0] for r in cur.fetchall())
    finally:
        conn.close()


def import_jobs(db_url, jobs):
    conn = psycopg2.connect(db_url)
    try:
        cur = conn.cursor()
        inserted = 0
        for job in jobs:
            cur.execute(
                """
                INSERT INTO "Job" (title, company, location, country, salary, "jobType",
                                    "visaSponsored", description, "applicationUrl", category, "createdAt")
                VALUES (%s, %s, %s, 'uk', %s, %s, true, %s, %s, 'Healthcare', NOW())
                """,
                (
                    job["title"], job["employer_trust"], job["location"],
                    job["salary"] or "Competitive", job["job_type"] or None,
                    job["description"], job["applicationUrl"],
                ),
            )
            inserted += 1
        conn.commit()
        return inserted
    finally:
        conn.close()


def run_occupation_code_backfill():
    # Repo root is two levels up from this script's own directory
    # (scripts/nhs-band3-weekly-import/) -- cwd matters here because the
    # .mjs script's own rootDir is self-resolving, but Node needs to be
    # invoked from somewhere sane and "scripts/backfill-occupation-codes.mjs"
    # (the same relative path import_jobs.py's own version uses) is only
    # valid relative to the repo root.
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    script = os.path.join("scripts", "backfill-occupation-codes.mjs")
    if not os.path.exists(os.path.join(repo_root, script)):
        print("WARNING: backfill-occupation-codes.mjs not found -- skipping occupation-code classification.")
        return
    print("\nClassifying occupation codes for newly-imported jobs...")
    result = subprocess.run(["node", script], cwd=repo_root, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"WARNING: occupation-code backfill exited with an error:\n{result.stderr}")


def write_report(rows):
    fieldnames = [
        "job_reference", "title", "employer_trust", "applicationUrl", "band",
        "sponsorship_eligible", "sponsorship_match_text", "duration_months",
        "strict_gate_passed", "strict_gate_reason", "already_live", "imported",
    ]
    with open(REPORT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    dry_run = "--dry-run" in sys.argv
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None
    db_url = get_database_url()

    print(f"Run started {datetime.now().isoformat()} | dry_run={dry_run}\n")

    live_urls = get_live_nhs_urls(db_url)
    print(f"{len(live_urls)} NHS jobs already live (will be skipped as duplicates).\n")

    refs = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            ignore_https_errors=True,
        )
        context.route(
            "**/*",
            lambda route: route.abort() if route.request.resource_type in ("image", "media", "font") else route.continue_(),
        )
        page = context.new_page()

        for pay_band in PAY_BANDS:
            page_num = 1
            total_count = None
            while True:
                url = search_results_url(page_num, pay_band)
                try:
                    goto_with_retry(page, url)
                except Exception as e:
                    print(f"ABORT: could not load {pay_band} search page {page_num} after retries: {e}")
                    sys.exit(1)
                page.wait_for_timeout(500)
                search_html = page.content()
                if total_count is None:
                    total_count = get_total_result_count(search_html)
                    print(f"NHS Jobs reports {total_count} {pay_band} results.\n")
                    min_expected = MIN_EXPECTED_RESULTS[pay_band]
                    if not total_count or total_count < min_expected:
                        print(f"ABORT: {total_count} is well below the normal range for {pay_band}. "
                              "The site layout may have changed or this request may be blocked. "
                              "Not importing anything from a run that can't trust its own result count.")
                        sys.exit(1)

                results = parse_search_results(search_html)
                if not results:
                    break
                for ref in results:
                    if ref not in refs:
                        refs.append(ref)
                if limit is not None and len(refs) >= limit:
                    refs = refs[:limit]
                    break
                page_num += 1
                if page_num > 200:
                    break
                time.sleep(0.5)

            print(f"Collected {len(refs)} job reference(s) so far after {pay_band} "
                  f"({page_num - 1} page(s)).\n")
            if limit is not None and len(refs) >= limit:
                break

        report_rows = []
        to_import = []
        for i, ref in enumerate(refs):
            job_url = f"{BASE_URL}/candidate/jobadvert/{ref}"
            try:
                goto_with_retry(page, job_url)
                page.wait_for_timeout(400)
                job = parse_job_detail(ref, page.content())
            except Exception as e:
                print(f"[{i+1}/{len(refs)}] {ref} -- failed to fetch/parse: {e}")
                continue

            already_live = job["applicationUrl"] in live_urls
            passed, reason = strict_auto_import_check(job) if not already_live else (False, "already live")

            print(f"[{i+1}/{len(refs)}] "
                  f"{'ALREADY LIVE' if already_live else ('PASS' if passed else 'HELD BACK')} "
                  f"-- {job['title']} @ {job['employer_trust']} ({reason})")

            report_rows.append({
                "job_reference": ref,
                "title": job["title"],
                "employer_trust": job["employer_trust"],
                "applicationUrl": job["applicationUrl"],
                "band": job["band"],
                "sponsorship_eligible": job["sponsorship_eligible"],
                "sponsorship_match_text": job["sponsorship_match_text"],
                "duration_months": job["duration_months"],
                "strict_gate_passed": passed,
                "strict_gate_reason": reason,
                "already_live": already_live,
                "imported": False,  # updated below if it actually gets inserted
            })
            if passed and not already_live:
                to_import.append(job)

            time.sleep(0.5)

        browser.close()

    print(f"\n{len(to_import)} job(s) passed the strict gate and are new.\n")

    if len(to_import) > MAX_IMPORT_PER_RUN:
        print(f"ABORT: {len(to_import)} jobs would be imported, above the "
              f"{MAX_IMPORT_PER_RUN} sanity cap for a single run. More likely a dedup or "
              f"scrape bug than a real surge. Writing the report but importing nothing.")
        write_report(report_rows)
        sys.exit(1)

    write_report(report_rows)
    print(f"Report written to {REPORT_CSV}.")

    if dry_run:
        print("\nDry run -- nothing imported.")
        return

    if not to_import:
        print("\nNothing to import.")
        return

    inserted = import_jobs(db_url, to_import)
    print(f"\nInserted {inserted} new NHS job(s).")

    imported_urls = {j["applicationUrl"] for j in to_import}
    for row in report_rows:
        if row["applicationUrl"] in imported_urls:
            row["imported"] = True
    write_report(report_rows)

    run_occupation_code_backfill()


if __name__ == "__main__":
    main()
