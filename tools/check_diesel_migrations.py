#!/usr/bin/env python3
"""Diagnose Diesel's generic Invalid migration directory error without a DB."""
import argparse
from pathlib import Path
import re
import sys

NAME = re.compile(r"(?:[0-9]{14}|[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{6}(?:-[0-9]{4})?)_.+")


def validate(root: Path) -> list[str]:
    errors = []
    versions = {}
    for path in sorted(root.iterdir()):
        if path.name.startswith(".") or path.is_file():
            continue
        if not path.is_dir():
            errors.append(f"{path}: not a migration directory")
            continue
        if not NAME.fullmatch(path.name):
            errors.append(f"{path}: unexpected migration name")
        version = path.name.split("_", 1)[0].replace("-", "")
        if version in versions:
            errors.append(f"{path}: duplicate Diesel version {version} (also {versions[version]})")
        versions[version] = path
        up = path / "up.sql"
        if not up.is_file():
            errors.append(f"{path}: missing up.sql (move unrelated directories outside migrations)")
        else:
            try:
                with up.open("rb") as stream:
                    stream.read(1)
            except OSError as exc:
                errors.append(f"{up}: {exc}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, nargs="?",
                        default=Path(__file__).resolve().parents[1] / "migrations")
    args = parser.parse_args()
    try:
        errors = validate(args.directory)
    except OSError as exc:
        errors = [str(exc)]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Migration directory layout OK: {args.directory}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
