import os
import re
import argparse
from collections import defaultdict

# Known episode counts per season (South Park S01–S27)
EPISODES_PER_SEASON = {
    1: 13, 2: 18, 3: 17, 4: 17, 5: 14, 6: 17, 7: 15, 8: 14,
    9: 14, 10: 14, 11: 14, 12: 14, 13: 14, 14: 14, 15: 14,
    16: 14, 17: 10, 18: 10, 19: 10, 20: 10, 21: 10, 22: 10,
    23: 10, 24: 2,  25: 6,  26: 6,  27: 6,
}

PATTERN = re.compile(
    r'^South_Park_English_(\d{2})_(\d{2})_.*\.mp4$',
    re.IGNORECASE
)


def scan_directory(directory: str) -> dict[tuple[int, int], str]:
    """Walk the directory tree and return a dict of (season, episode) -> filename."""
    found = {}
    for root, _, files in os.walk(directory):
        for fname in files:
            m = PATTERN.match(fname)
            if m:
                season = int(m.group(1))
                episode = int(m.group(2))
                found[(season, episode)] = fname
    return found


def find_missing(found: dict) -> dict[int, list[int]]:
    """Compare found episodes against known counts and return missing ones."""
    # Collect all seasons: those we have files for + those in the known list
    all_seasons = set(EPISODES_PER_SEASON) | {s for s, _ in found}
    missing = defaultdict(list)

    for season in sorted(all_seasons):
        total = EPISODES_PER_SEASON.get(season, max(
            (ep for s, ep in found if s == season), default=0
        ))
        for ep in range(1, total + 1):
            if (season, ep) not in found:
                missing[season].append(ep)

    return dict(missing)


def report(found: dict, missing: dict, directory: str) -> None:
    all_seasons = sorted(set(EPISODES_PER_SEASON) | set(missing) | {s for s, _ in found})

    print(f"\n{'='*60}")
    print(f"  South Park Episode Scanner")
    print(f"  Directory : {directory}")
    print(f"  Files found: {len(found)}")
    print(f"{'='*60}\n")

    total_missing = 0

    for season in all_seasons:
        expected = EPISODES_PER_SEASON.get(season, "?")
        season_found = [(s, e) for s, e in found if s == season]
        missing_eps = missing.get(season, [])
        total_missing += len(missing_eps)

        status = "✓ Complete" if not missing_eps else f"✗ {len(missing_eps)} missing"
        print(f"  Season {season:02d}  [{len(season_found):2d}/{expected} episodes]  {status}")

        if missing_eps:
            # Format as compact ranges where possible
            ranges = []
            start = end = missing_eps[0]
            for ep in missing_eps[1:]:
                if ep == end + 1:
                    end = ep
                else:
                    ranges.append(f"{start}" if start == end else f"{start}-{end}")
                    start = end = ep
            ranges.append(f"{start}" if start == end else f"{start}-{end}")
            print(f"           Missing episodes: {', '.join(ranges)}")

    print(f"\n{'='*60}")
    print(f"  Total missing: {total_missing} episode(s)")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Scan a directory for South Park episodes and list missing ones."
    )
    parser.add_argument(
        "directory",
        nargs="?",
        default=".",
        help="Path to the directory containing episode files (default: current dir)"
    )
    args = parser.parse_args()

    directory = os.path.abspath(args.directory)
    if not os.path.isdir(directory):
        print(f"Error: '{directory}' is not a valid directory.")
        raise SystemExit(1)

    found = scan_directory(directory)
    missing = find_missing(found)
    report(found, missing, directory)


if __name__ == "__main__":
    main()