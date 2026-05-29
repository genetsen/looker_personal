#!/usr/bin/env python3
"""Build a review CSV that maps master-model supplier names to logo URLs.

The script reads distinct `_supplier_name` values from the live master data
model, resolves each supplier to a likely web domain, and writes a local CSV
for review. It does not write to BigQuery or replace the live curated lookup.
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable


PROJECT_ID = "looker-studio-pro-452620"
MASTER_TABLE = f"{PROJECT_ID}.master_stg.data_model"
HUNTER_LOGO_BASE_URL = "https://logos.hunter.io/"
DEFAULT_OUTPUT_PATH = Path(__file__).with_name("supplier_logo_mapping_review.csv")

ALIAS_DOMAINS = {
    "abc": "abc.com",
    "advisor perspectives": "advisorperspectives.com",
    "bloo": "bloomberg.com",
    "cnbc": "cnbc.com",
    "disney-hulu": "hulu.com",
    "dse": "debeersgroup.com",
    "fxbs": "fox.com",
    "golf": "golf.com",
    "google_ads": "google.com",
    "hulu": "hulu.com",
    "linkedin": "linkedin.com",
    "meta": "meta.com",
    "mindbodygreen": "mindbodygreen.com",
    "morning brew": "morningbrew.com",
    "nbc": "nbc.com",
    "netflix ads": "netflix.com",
    "pinterest": "pinterest.com",
    "playfly": "playfly.com",
    "smart brief": "smartbrief.com",
    "smartbrief": "smartbrief.com",
    "tiktok": "tiktok.com",
    "youtube": "youtube.com",
}

DOMAIN_SUFFIXES = (
    "media",
    "magazine",
    "company",
    "inc",
    "llc",
    "ltd",
    "corp",
    "corporation",
    "group",
    "ads",
)


@dataclass(frozen=True)
class SupplierStats:
    supplier_name: str
    row_count: int
    missing_logo_row_count: int


@dataclass(frozen=True)
class LogoResolution:
    supplier_name: str
    resolved_domain: str
    logo_url_final: str
    resolution_method: str
    confidence: str
    needs_review: str


def normalize_supplier_key(supplier_name: str) -> str:
    return re.sub(r"\s+", " ", supplier_name.strip().lower())


def clean_domain(value: str | None) -> str:
    if not value:
        return ""

    parsed = urllib.parse.urlparse(value if "://" in value else f"https://{value}")
    domain = parsed.netloc or parsed.path.split("/")[0]
    domain = domain.lower().strip().strip(".")
    if domain.startswith("www."):
        domain = domain[4:]

    return domain


def hunter_logo_url(domain: str) -> str:
    return f"{HUNTER_LOGO_BASE_URL}{domain}"


def guess_domain(supplier_name: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", supplier_name.lower())
    tokens = [token for token in tokens if token not in DOMAIN_SUFFIXES]
    if not tokens:
        return ""
    return f"{''.join(tokens)}.com"


def search_first_result_domain(supplier_name: str, timeout_seconds: int = 10) -> str | None:
    query = urllib.parse.urlencode({"q": f"{supplier_name} official website"})
    url = f"https://duckduckgo.com/html/?{query}"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 supplier-logo-mapping-review/1.0"},
    )

    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        html = response.read().decode("utf-8", errors="replace")

    hrefs = re.findall(r'class="result__a"[^>]+href="([^"]+)"', html)
    for href in hrefs:
        parsed_href = html_unescape(href)
        parsed = urllib.parse.urlparse(parsed_href)
        if parsed.netloc == "duckduckgo.com":
            query_params = urllib.parse.parse_qs(parsed.query)
            redirect_target = query_params.get("uddg", [""])[0]
            domain = clean_domain(urllib.parse.unquote(redirect_target))
        else:
            domain = clean_domain(parsed_href)
        if domain:
            return domain

    return None


def html_unescape(value: str) -> str:
    return (
        value.replace("&amp;", "&")
        .replace("&quot;", '"')
        .replace("&#x2F;", "/")
        .replace("&#39;", "'")
    )


def resolve_supplier(
    supplier_name: str,
    search_domain: Callable[[str], str | None] | None = None,
) -> LogoResolution:
    key = normalize_supplier_key(supplier_name)
    alias_domain = ALIAS_DOMAINS.get(key)
    if alias_domain:
        domain = clean_domain(alias_domain)
        return LogoResolution(
            supplier_name=supplier_name,
            resolved_domain=domain,
            logo_url_final=hunter_logo_url(domain),
            resolution_method="alias",
            confidence="high",
            needs_review="false",
        )

    if search_domain:
        try:
            searched_domain = clean_domain(search_domain(supplier_name))
        except (OSError, TimeoutError, ValueError):
            searched_domain = ""
        if searched_domain:
            return LogoResolution(
                supplier_name=supplier_name,
                resolved_domain=searched_domain,
                logo_url_final=hunter_logo_url(searched_domain),
                resolution_method="search_first_result",
                confidence="medium",
                needs_review="true",
            )

    guessed_domain = guess_domain(supplier_name)
    if guessed_domain:
        return LogoResolution(
            supplier_name=supplier_name,
            resolved_domain=guessed_domain,
            logo_url_final=hunter_logo_url(guessed_domain),
            resolution_method="domain_guess",
            confidence="low",
            needs_review="true",
        )

    return LogoResolution(
        supplier_name=supplier_name,
        resolved_domain="",
        logo_url_final="",
        resolution_method="unresolved",
        confidence="none",
        needs_review="true",
    )


def fetch_supplier_stats(limit: int | None = None, only_missing: bool = True) -> list[SupplierStats]:
    having_clause = "HAVING missing_logo_row_count > 0" if only_missing else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    sql = f"""
SELECT
  _supplier_name AS supplier_name,
  COUNT(*) AS row_count,
  COUNTIF(_supplier_logo IS NULL OR TRIM(_supplier_logo) = '') AS missing_logo_row_count
FROM `{MASTER_TABLE}`
WHERE _supplier_name IS NOT NULL
  AND TRIM(_supplier_name) != ''
GROUP BY _supplier_name
{having_clause}
ORDER BY missing_logo_row_count DESC, row_count DESC, supplier_name
{limit_clause}
""".strip()

    command = [
        "bq",
        "query",
        f"--project_id={PROJECT_ID}",
        "--use_legacy_sql=false",
        "--format=csv",
        sql,
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return parse_supplier_stats_csv(result.stdout)


def parse_supplier_stats_csv(csv_text: str) -> list[SupplierStats]:
    rows: list[SupplierStats] = []
    reader = csv.DictReader(csv_text.splitlines())
    for row in reader:
        supplier_name = (row.get("supplier_name") or "").strip()
        if not supplier_name:
            continue
        rows.append(
            SupplierStats(
                supplier_name=supplier_name,
                row_count=int(row["row_count"]),
                missing_logo_row_count=int(row["missing_logo_row_count"]),
            )
        )
    return rows


def write_review_csv(
    suppliers: Iterable[SupplierStats],
    output_path: Path,
    search_domain: Callable[[str], str | None] | None = None,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "supplier_name",
        "row_count",
        "missing_logo_row_count",
        "resolved_domain",
        "logo_url_final",
        "resolution_method",
        "confidence",
        "needs_review",
    ]
    written_count = 0

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for supplier in suppliers:
            resolution = resolve_supplier(supplier.supplier_name, search_domain=search_domain)
            writer.writerow(
                {
                    "supplier_name": supplier.supplier_name,
                    "row_count": supplier.row_count,
                    "missing_logo_row_count": supplier.missing_logo_row_count,
                    "resolved_domain": resolution.resolved_domain,
                    "logo_url_final": resolution.logo_url_final,
                    "resolution_method": resolution.resolution_method,
                    "confidence": resolution.confidence,
                    "needs_review": resolution.needs_review,
                }
            )
            written_count += 1

    return written_count


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a review CSV mapping master-model supplier names to Hunter logo URLs."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Review CSV path. Default: {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional supplier limit for quick review runs.",
    )
    parser.add_argument(
        "--all-suppliers",
        action="store_true",
        help="Include suppliers even when they already have logo values.",
    )
    parser.add_argument(
        "--disable-web-search",
        action="store_true",
        help="Skip DuckDuckGo HTML search and use aliases/domain guesses only.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    suppliers = fetch_supplier_stats(limit=args.limit, only_missing=not args.all_suppliers)
    search_domain = None if args.disable_web_search else search_first_result_domain
    written_count = write_review_csv(suppliers, args.output, search_domain=search_domain)

    print(f"Wrote {written_count} supplier logo review rows to {args.output}")
    print("No BigQuery tables were modified.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
