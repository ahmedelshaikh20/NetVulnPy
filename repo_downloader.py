"""
repo_downloader.py  —  Phase 1: Download & Extract

Reads repos.json produced by github_repo_harvester.py, downloads each repo
as a GitHub zipball, and extracts only .py files to a local directory.

Usage:
    python repo_downloader.py [--input repos.json] [--output-dir DIR]
                            [--token TOKEN] [--limit N]
                            [--keep-files] [--verbose]
"""

import io
import os
import shutil
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

from github_repo_harvester import get_session

load_dotenv()

ZIPBALL_URL = "https://api.github.com/repos/{full_name}/zipball"
MAX_RETRIES = 5


def load_repos(input_path, limit):
    import json

    if not os.path.exists(input_path):
        print(f"Error: '{input_path}' not found. Run github_repo_harvester.py first.", file=sys.stderr)
        sys.exit(1)
    with open(input_path, encoding="utf-8") as f:
        try:
            repos = json.load(f)
        except json.JSONDecodeError as exc:
            print(f"Error: '{input_path}' is not valid JSON: {exc}", file=sys.stderr)
            sys.exit(1)
    if not isinstance(repos, list):
        print(f"Error: '{input_path}' must be a JSON array.", file=sys.stderr)
        sys.exit(1)
    if limit is not None:
        repos = repos[:limit]
    return repos


def download_zip(session, full_name, zip_path, verbose):
    """Download the zipball for a repo to zip_path. Returns True on success."""
    url = ZIPBALL_URL.format(full_name=full_name)
    authenticated = "Authorization" in session.headers

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, stream=True, allow_redirects=True, timeout=(10, 60))

            if resp.status_code in (403, 429):
                retry_after = int(resp.headers.get("Retry-After", 60))
                if verbose:
                    print(f"    Rate limited. Sleeping {retry_after + 1}s ...", file=sys.stderr)
                time.sleep(retry_after + 1)
                continue

            resp.raise_for_status()

            os.makedirs(os.path.dirname(zip_path), exist_ok=True)
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True

        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code >= 500:
                wait = 2 ** attempt
                if verbose:
                    print(f"    Server error ({exc.response.status_code}). Retrying in {wait}s ...", file=sys.stderr)
                time.sleep(wait)
            else:
                if verbose:
                    print(f"    HTTP error for {full_name}: {exc}", file=sys.stderr)
                return False
        except requests.RequestException as exc:
            wait = 2 ** attempt
            if verbose:
                print(f"    Network error. Retrying in {wait}s ...", file=sys.stderr)
            time.sleep(wait)

    if verbose:
        print(f"    Failed to download {full_name} after {MAX_RETRIES} retries.", file=sys.stderr)
    return False


def extract_py_files(zip_path, extract_dir):
    """
    Extract only .py files from zip_path into extract_dir.
    Strips GitHub's top-level directory prefix (e.g. owner-repo-sha/).
    Returns the count of .py files extracted.
    """
    count = 0
    with zipfile.ZipFile(zip_path, "r") as zf:
        all_names = zf.namelist()

        # GitHub zips always have a single root dir — detect and strip it
        root_prefix = ""
        if all_names:
            first = all_names[0]
            if "/" in first:
                root_prefix = first.split("/")[0] + "/"

        for member in all_names:
            if not member.endswith(".py"):
                continue

            # Strip the root prefix
            stripped = member[len(root_prefix):] if member.startswith(root_prefix) else member

            # Path traversal guard
            if not stripped or stripped.startswith("..") or os.path.isabs(stripped):
                continue

            target_path = os.path.join(extract_dir, stripped)
            os.makedirs(os.path.dirname(target_path), exist_ok=True)

            with zf.open(member) as src, open(target_path, "wb") as dst:
                dst.write(src.read())

            count += 1

    return count


def cleanup(zip_path, extract_dir):
    if os.path.exists(zip_path):
        os.remove(zip_path)
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir, ignore_errors=True)


def _download_one(token, full_name, downloads_dir, keep_files, verbose, page_delay):
    """Download and extract a single repo. Returns (full_name, success, py_count)."""
    session = get_session(token)  # thread-local session

    safe_name = full_name.replace("/", "_")
    zip_path = os.path.join(downloads_dir, f"{safe_name}.zip")
    extract_dir = os.path.join(downloads_dir, safe_name)

    ok = download_zip(session, full_name, zip_path, verbose)
    if not ok:
        return (full_name, False, 0)

    py_count = extract_py_files(zip_path, extract_dir)

    if not keep_files:
        if os.path.exists(zip_path):
            os.remove(zip_path)

    time.sleep(page_delay)
    return (full_name, True, py_count)


def run(args):
    token = args.token or os.getenv("GITHUB_TOKEN")
    if not token:
        print(
            "Warning: No token provided. Using unauthenticated requests "
            "(60 req/hour limit). Set GITHUB_TOKEN or use --token.",
            file=sys.stderr,
        )

    authenticated = token is not None
    page_delay = 1.0 if authenticated else 5.0
    workers = getattr(args, "workers", 4)

    repos = load_repos(args.input, args.limit)
    downloads_dir = os.path.join(args.output_dir, "downloads")
    os.makedirs(downloads_dir, exist_ok=True)

    total = len(repos)
    succeeded = 0
    failed = 0

    if args.verbose:
        print(f"Processing {total} repos with {workers} worker(s) ...", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for i, repo in enumerate(repos, 1):
            full_name = repo.get("full_name", "")
            if not full_name:
                failed += 1
                continue
            fut = pool.submit(
                _download_one, token, full_name, downloads_dir,
                args.keep_files, args.verbose, page_delay,
            )
            futures[fut] = (i, full_name)

        for fut in as_completed(futures):
            i, full_name = futures[fut]
            try:
                _, ok, py_count = fut.result()
                if ok:
                    succeeded += 1
                    if args.verbose:
                        print(f"[{succeeded + failed}/{total}] {full_name} — {py_count} .py files", file=sys.stderr)
                else:
                    failed += 1
            except Exception as exc:
                if args.verbose:
                    print(f"[{succeeded + failed}/{total}] {full_name} — error: {exc}", file=sys.stderr)
                failed += 1

    print(f"\nDone. {succeeded}/{total} repos downloaded and extracted.")
    print(f"Extracted files are in: {downloads_dir}")
    if failed:
        print(f"Failed: {failed} repos (see warnings above).")


