#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import urlopen

STABLE_RELEASE = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Emit a GitHub Actions matrix for Pydantic compatibility tests. "
            "The matrix includes exact fixed versions and the latest patch "
            "release for the most recent minor lines."
        )
    )
    parser.add_argument("--package", default="pydantic")
    parser.add_argument(
        "--major",
        type=int,
        default=2,
        help="Only include releases from this major version. Default: 2",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of minor lines to include. Default: 25",
    )
    parser.add_argument(
        "--fixed",
        nargs="*",
        default=[],
        metavar="VERSION",
        help=(
            "Exact stable versions to include in addition to the rolling minor "
            "matrix, for example: --fixed 2.10.6 2.11.0 2.12.5"
        ),
    )
    parser.add_argument(
        "--releases-file",
        type=Path,
        help=(
            "Load release metadata from a local JSON file instead of PyPI. "
            "The file may contain the full PyPI JSON payload or only a "
            "'releases' mapping."
        ),
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print the JSON output.",
    )
    return parser.parse_args()


def parse_stable_version(version: str) -> tuple[int, int, int] | None:
    match = STABLE_RELEASE.fullmatch(version)
    if match is None:
        return None

    return (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
    )


def is_yanked_release(files: Any) -> bool:
    if not isinstance(files, list) or not files:
        return False

    yanked_flags = [
        file_info.get("yanked", False)
        for file_info in files
        if isinstance(file_info, dict)
    ]
    return bool(yanked_flags) and all(yanked_flags)


def load_release_map(package: str, releases_file: Path | None) -> dict[str, Any]:
    if releases_file is not None:
        payload = json.loads(releases_file.read_text())
    else:
        url = f"https://pypi.org/pypi/{quote(package)}/json"
        with urlopen(url, timeout=30) as response:
            payload = json.load(response)

    if isinstance(payload, dict) and "releases" in payload:
        releases = payload["releases"]
    else:
        releases = payload

    if not isinstance(releases, dict):
        raise ValueError("Expected a JSON object containing a 'releases' mapping.")

    return releases


def latest_patch_by_minor(
    release_map: dict[str, Any], major: int, limit: int
) -> list[tuple[int, int, int]]:
    latest_by_minor: dict[tuple[int, int], tuple[int, int, int]] = {}

    for version, files in release_map.items():
        parsed = parse_stable_version(version)
        if parsed is None or parsed[0] != major or is_yanked_release(files):
            continue

        key = parsed[:2]
        current = latest_by_minor.get(key)
        if current is None or parsed > current:
            latest_by_minor[key] = parsed

    return sorted(latest_by_minor.values(), reverse=True)[:limit]


def build_matrix(
    package: str,
    major: int,
    fixed_versions: list[str],
    rolling_versions: list[tuple[int, int, int]],
    release_map: dict[str, Any],
) -> dict[str, list[dict[str, str]]]:
    include: list[dict[str, str]] = []
    seen_fixed: set[str] = set()

    for version in fixed_versions:
        parsed = parse_stable_version(version)
        if parsed is None:
            raise ValueError(
                f"Fixed version '{version}' must be a stable X.Y.Z release."
            )
        if parsed[0] != major:
            raise ValueError(
                f"Fixed version '{version}' is outside the configured major line "
                f"{major}.x."
            )
        if version not in release_map:
            raise ValueError(f"Fixed version '{version}' was not found on PyPI.")
        if version in seen_fixed:
            continue

        seen_fixed.add(version)
        include.append(
            {
                "label": f"fixed-{version}",
                "kind": "fixed",
                "specifier": f"=={version}",
                "resolved_version": version,
                "package": package,
            }
        )

    for major, minor, patch in rolling_versions:
        minor_label = f"{major}.{minor}"
        include.append(
            {
                "label": f"minor-{minor_label}",
                "kind": "minor",
                "specifier": f"=={minor_label}.*",
                "resolved_version": f"{major}.{minor}.{patch}",
                "package": package,
            }
        )

    return {"include": include}


def main() -> int:
    args = parse_args()
    release_map = load_release_map(args.package, args.releases_file)
    rolling_versions = latest_patch_by_minor(release_map, args.major, args.limit)

    if not rolling_versions:
        raise ValueError(
            f"No stable {args.package} {args.major}.x releases were found on PyPI."
        )

    matrix = build_matrix(
        package=args.package,
        major=args.major,
        fixed_versions=args.fixed,
        rolling_versions=rolling_versions,
        release_map=release_map,
    )

    json.dump(matrix, sys.stdout, indent=2 if args.pretty else None)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        sys.stderr.write(f"{exc}\n")
        raise SystemExit(1)
