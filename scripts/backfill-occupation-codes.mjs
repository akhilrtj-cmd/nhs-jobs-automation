// Classifies occupationCode for every visa-sponsored NHS job this repo's
// own import script (scripts/nhs-band3-weekly-import/extract_classify_import.py)
// just inserted with no code yet. Ported from the main Trabajo app repo's
// own copy of this script -- kept deliberately self-contained here (just
// @neondatabase/serverless, ws, and this repo's own copy of the occupation
// data file) rather than reaching back into that repo, so this automation
// has no hidden cross-repo dependency.
//
// The main repo's version also chains into a "tag-intl-eligibility.mjs"
// step afterward (an unrelated feature -- tagging jobs as suitable for
// international candidates). That step has its own deep dependency chain
// (a Prisma adapter, csv-stringify, several app/lib modules) that isn't
// worth replicating here for a feature nobody asked this repo to run; this
// copy stops after occupation-code classification.
//
// Usage: node scripts/backfill-occupation-codes.mjs [--limit N]
//
// Uses OPENAI_API_KEY and DATABASE_URL from the environment (set via
// GitHub Actions secrets in this repo's workflow, or from .env locally).

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { Pool, neonConfig } from "@neondatabase/serverless";
import ws from "ws";

const rootDir = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

function loadEnv() {
  const envPath = path.join(rootDir, ".env");
  const text = readFileSync(envPath, "utf-8");
  for (const line of text.split(/\r?\n/)) {
    const m = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (!m) continue;
    const key = m[1];
    let val = m[2];
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    if (!(key in process.env)) process.env[key] = val;
  }
}
loadEnv();

neonConfig.webSocketConstructor = ws;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const occupationsData = JSON.parse(
  readFileSync(path.join(rootDir, "app", "lib", "data", "skilledWorkerOccupations.json"), "utf-8")
);
const occupations = occupationsData.occupations;
const byCode = new Map(occupations.map((o) => [o.code, o]));
const listText = occupations.map((o) => `${o.code} | ${o.jobType} | ${o.relatedTitles}`).join("\n");

const MODEL = "gpt-6-luna";
// gpt-6-luna prices, USD per 1M tokens (OpenAI pricing page, 6 Oct 2026).
const PRICE = { input: 0.1, cachedInput: 0.01, output: 0.5 };
const SYSTEM_PROMPT =
  "You classify a UK job posting against the Home Office's official Skilled Worker visa " +
  "occupation code list. You are given the full list as `CODE | job type | related job titles` " +
  "rows, one per line, followed by a job posting's title and a short description. Return the " +
  "single code whose job type or related titles best matches what this job actually IS -- not " +
  "just a loosely related field. If nothing on the list is a confident, reasonable match, return " +
  "null rather than guessing -- an incorrect code on a visa-eligibility page would mislead the " +
  "person reading it.";

// Mirrors the main app's own occupation classifier's extractPlainText --
// job descriptions are stored as raw scraped HTML, and NHS Jobs adverts in
// particular carry a fair amount of markup around the real text, so tags
// are stripped before truncating rather than after, to spend the character
// budget on visible words instead of markup noise.
function extractPlainText(html) {
  return (html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

// One retry on a rate limit or a 5xx.
async function classify(title, description) {
  const body = JSON.stringify({
    model: MODEL,
    reasoning: { effort: "low" },
    max_output_tokens: 1500,
    input: [
      { role: "system", content: SYSTEM_PROMPT },
      {
        role: "user",
        content:
          `OCCUPATION LIST:\n${listText}\n\n` +
          `JOB TITLE: ${title}\n\n` +
          `JOB DESCRIPTION (may be truncated):\n${extractPlainText(description).slice(0, 2000)}`,
      },
    ],
    text: {
      format: {
        type: "json_schema",
        name: "occupation_match",
        strict: true,
        schema: {
          type: "object",
          properties: {
            code: {
              type: ["string", "null"],
              description: 'The 4-digit occupation code, e.g. "2455", or null if there is no confident match.',
            },
          },
          required: ["code"],
          additionalProperties: false,
        },
      },
    },
  });

  let response;
  for (let attempt = 1; attempt <= 2; attempt++) {
    response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}`, "Content-Type": "application/json" },
      signal: AbortSignal.timeout(60_000),
      body,
    });
    if (response.ok || (response.status !== 429 && response.status < 500) || attempt === 2) break;
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
  if (!response.ok) throw new Error(`OpenAI ${response.status}: ${(await response.text()).slice(0, 200)}`);

  const result = await response.json();
  const text = result.output?.find((o) => o.type === "message")?.content?.find((c) => c.type === "output_text")?.text;
  if (!text) throw new Error(`no answer in the response (status: ${result.status})`);
  const parsed = JSON.parse(text);
  const code = parsed.code && byCode.has(parsed.code) ? parsed.code : null;
  return { code, usage: result.usage || {} };
}

async function main() {
  const limitArgIdx = process.argv.indexOf("--limit");
  const limit = limitArgIdx !== -1 ? parseInt(process.argv[limitArgIdx + 1], 10) : null;

  // Internship, graduate and early-careers roles deliberately carry no
  // occupation code, matching the main app's own convention.
  const EXCLUDED_TITLE_PATTERN = '\\y(interns?|internships?|graduates?|early careers?)\\y';
  let query =
    'SELECT id, title, description, "occupationCode" FROM "Job" WHERE "visaSponsored" = true AND "occupationCode" IS NULL AND title !~* $1 ORDER BY id';
  const params = [EXCLUDED_TITLE_PATTERN];
  if (limit) query += ` LIMIT ${limit}`;
  const { rows } = await pool.query(query, params);

  console.log(`Found ${rows.length} visa-sponsored job(s) with no occupation code.`);

  let matched = 0;
  let unmatched = 0;
  let errored = 0;
  const totalUsage = { input_tokens: 0, cached_input_tokens: 0, output_tokens: 0 };
  for (const [i, row] of rows.entries()) {
    try {
      const { code, usage } = await classify(row.title, row.description);
      totalUsage.input_tokens += usage.input_tokens || 0;
      totalUsage.cached_input_tokens += usage.input_tokens_details?.cached_tokens || 0;
      totalUsage.output_tokens += usage.output_tokens || 0;
      if (i < 3) console.log(`  usage for job ${i + 1}:`, JSON.stringify(usage));

      if (code) {
        await pool.query('UPDATE "Job" SET "occupationCode" = $1 WHERE id = $2', [code, row.id]);
        matched++;
        console.log(`  [${row.id}] "${row.title}" -> ${code} (${byCode.get(code).jobType})`);
      } else {
        unmatched++;
        console.log(`  [${row.id}] "${row.title}" -> no confident match`);
      }
    } catch (err) {
      errored++;
      console.error(`  [${row.id}] "${row.title}" -> ERROR: ${err.message}`);
    }
  }

  const uncachedInput = totalUsage.input_tokens - totalUsage.cached_input_tokens;
  const cost =
    (uncachedInput / 1_000_000) * PRICE.input +
    (totalUsage.cached_input_tokens / 1_000_000) * PRICE.cachedInput +
    (totalUsage.output_tokens / 1_000_000) * PRICE.output;

  console.log(`\nDone. Matched ${matched}, no confident match ${unmatched}, errored ${errored}, total ${rows.length}.`);
  console.log(`Token usage:`, JSON.stringify(totalUsage));
  console.log(`Estimated cost: $${cost.toFixed(4)}`);
  await pool.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
