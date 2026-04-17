"""
github_repo_harvester.py

Fetches open-source Python repositories from the GitHub Search API
and saves results to repos.json and repos.csv.

Usage:
    python github_repo_harvester.py [--max-repos N] [--output-dir DIR]
                                    [--token TOKEN] [--per-page N] [--verbose]
"""

import argparse
import csv
import json
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()
if not os.getenv("GITHUB_TOKEN"):
    load_dotenv(dotenv_path=".env.example")

SEARCH_URL = "https://api.github.com/search/repositories"
MAX_API_RESULTS = 1000  # GitHub Search API hard cap
MAX_RETRIES = 5

FIELDNAMES = [
    "name",
    "full_name",
    "html_url",
    "description",
    "stars",
    "forks",
    "open_issues",
    "language",
    "topics",
    "created_at",
    "updated_at",
    "license",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Harvest open-source Python repos from GitHub."
    )
    parser.add_argument(
        "--max-repos",
        type=int,
        default=1000,
        help="Maximum number of repos to collect (capped at 1000 by the API).",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory to write repos.json and repos.csv (default: current directory).",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="GitHub personal access token. Falls back to GITHUB_TOKEN env var.",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        choices=range(1, 101),
        metavar="1-100",
        help="Results per API page (default: 100).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress messages to stderr.",
    )
    return parser.parse_args()


def get_session(token):
    session = requests.Session()
    session.headers.update(
        {
            "Accept": (
                "application/vnd.github+json, "
                "application/vnd.github.mercy-preview+json"
            ),
            "User-Agent": "github-repo-harvester/1.0",
        }
    )
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    return session


def search_page(session, page, per_page, verbose):
    params = {
        "q": "language:python",
        "sort": "stars",
        "order": "desc",
        "per_page": per_page,
        "page": page,
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(SEARCH_URL, params=params, timeout=15)
            if resp.status_code in (403, 429):
                retry_after = int(resp.headers.get("Retry-After", 60))
                if verbose:
                    print(
                        f"  Rate limited. Sleeping {retry_after + 1}s ...",
                        file=sys.stderr,
                    )
                time.sleep(retry_after + 1)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code >= 500:
                wait = 2 ** attempt
                if verbose:
                    print(f"  Server error. Retrying in {wait}s ...", file=sys.stderr)
                time.sleep(wait)
            else:
                raise
        except requests.RequestException:
            wait = 2 ** attempt
            if verbose:
                print(f"  Network error. Retrying in {wait}s ...", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch page {page} after {MAX_RETRIES} retries.")


def extract_fields(item):
    return {
        "name": item.get("name"),
        "full_name": item.get("full_name"),
        "html_url": item.get("html_url"),
        "description": item.get("description"),
        "stars": item.get("stargazers_count"),
        "forks": item.get("forks_count"),
        "open_issues": item.get("open_issues_count"),
        "language": item.get("language"),
        "topics": ";".join(item.get("topics") or []),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "license": (
            item["license"]["spdx_id"]
            if item.get("license")
            else None
        ),
    }


def fetch_all_repos(session, max_repos, per_page, verbose):
    records = []
    page = 1
    authenticated = "Authorization" in session.headers
    page_delay = 0.5 if authenticated else 2.0

    while len(records) < max_repos:
        if verbose:
            print(f"Fetching page {page} ...", file=sys.stderr)

        data = search_page(session, page, per_page, verbose)
        items = data.get("items", [])

        if not items:
            break

        for item in items:
            records.append(extract_fields(item))
            if len(records) >= max_repos:
                break

        if verbose:
            print(f"  Collected {len(records)} repos so far.", file=sys.stderr)

        if len(items) < per_page:
            break  # last page

        page += 1
        if len(records) < max_repos:
            time.sleep(page_delay)

    return records


def save_json(records, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)


def save_csv(records, path):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=FIELDNAMES, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(records)


def main():
    args = parse_args()

    max_repos = min(args.max_repos, MAX_API_RESULTS)
    if args.max_repos > MAX_API_RESULTS:
        print(
            f"Warning: --max-repos capped at {MAX_API_RESULTS} (GitHub API limit).",
            file=sys.stderr,
        )

    token = args.token or os.getenv("GITHUB_TOKEN")
    if not token:
        print(
            "Warning: No token provided. Using unauthenticated requests "
            "(10 req/min limit). Set GITHUB_TOKEN or use --token to increase limit.",
            file=sys.stderr,
        )

    session = get_session(token)

    if args.verbose:
        print(
            f"Harvesting up to {max_repos} Python repos from GitHub ...",
            file=sys.stderr,
        )

    records = fetch_all_repos(session, max_repos, args.per_page, args.verbose)

    os.makedirs(args.output_dir, exist_ok=True)
    json_path = os.path.join(args.output_dir, "repos.json")
    csv_path = os.path.join(args.output_dir, "repos.csv")

    save_json(records, json_path)
    save_csv(records, csv_path)

    print(f"Saved {len(records)} repos to:")
    print(f"  {json_path}")
    print(f"  {csv_path}")


if __name__ == "__main__":
    main()
