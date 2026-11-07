#!/usr/bin/env python3
"""Regenerate vendored jiff-tzdb from a pinned IANA data release (requires a C compiler and make)."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('version', help='IANA release, e.g. 2026f')
args = parser.parse_args()
if not re.fullmatch(r'\d{4}[a-z]+', args.version):
    parser.error('invalid IANA release')
root = Path(__file__).resolve().parents[1]
files = ['africa', 'antarctica', 'asia', 'australasia', 'europe', 'northamerica',
         'southamerica', 'etcetera', 'backward', 'factory']
url = f'https://data.iana.org/time-zones/releases/tzdata{args.version}.tar.gz'
data = urllib.request.urlopen(url, timeout=60).read()
with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        archive.extractall(tmp, filter='data')
    code_url = f'https://data.iana.org/time-zones/releases/tzcode{args.version}.tar.gz'
    code = urllib.request.urlopen(code_url, timeout=60).read()
    with tarfile.open(fileobj=io.BytesIO(code), mode='r:gz') as archive:
        archive.extractall(tmp, filter='data')
    subprocess.run(['make', 'zic'], cwd=tmp, check=True)
    output = tmp / 'zoneinfo' 
    # No -L: compile Unix/POSIX timestamps, without leap-second corrections.
    subprocess.run([str(tmp / 'zic'), '-b', 'slim', '-d', str(output),
                    *[str(tmp / name) for name in files]], check=True)
    blob = bytearray()
    seen = {}
    entries = []
    for path in sorted(output.rglob('*'), key=lambda p: p.relative_to(output).as_posix().lower()):
        if not path.is_file():
            continue
        content = path.read_bytes()
        if content not in seen:
            start = len(blob)
            blob.extend(content)
            seen[content] = (start, len(blob))
        start, end = seen[content]
        entries.append(f'    ({json.dumps(path.relative_to(output).as_posix())}, {start}..{end}),')
    dest = root / 'crates/jiff-tzdb'
    (dest / 'sources.json').write_text(json.dumps({'version': args.version, 'tzdata_sha256': hashlib.sha256(data).hexdigest(), 'tzcode_sha256': hashlib.sha256(code).hexdigest()}, indent=2) + '\n')
    (dest / 'concatenated-zoneinfo.dat').write_bytes(blob)
    (dest / 'tzname.rs').write_text(
        f'pub(super) static VERSION: Option<&str> = Some("{args.version}");\n'
        'pub(super) static TZNAME_TO_OFFSET: &[(&str, core::ops::Range<usize>)] = &[\n'
        + '\n'.join(entries) + '\n];\n')
print(f'Bundled tzdata {args.version}: {len(entries)} names, {len(blob)} bytes')
