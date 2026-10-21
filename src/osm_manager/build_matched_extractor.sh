#!/usr/bin/env bash
# Build osr-extract using the exact CMake checkout and pinned dependencies
# already compiled by osr-ffi. An upstream release can serialize routing.bin
# in a format incompatible with the reader bundled into the Rust executable.
set -euo pipefail

native_root="${CARGO_TARGET_DIR:-target}/release/build"
bridge="$(find "$native_root" -path '*/out/native-lib/libcatenary_osr_bridge.so' -type f -print -quit)"
if [[ -z "$bridge" ]]; then
  echo "No compiled osr-ffi bridge found; build Gentian or osr_worker first." >&2
  exit 1
fi

out_dir="${bridge%/native-lib/libcatenary_osr_bridge.so}"
cmake_dir="$out_dir/build"
test -f "$cmake_dir/CMakeCache.txt" || {
  echo "Missing osr-ffi CMake build tree at $cmake_dir" >&2
  exit 1
}

echo "Building osr-extract from the same OSR source and CMake cache as osr-ffi:"
git -C "$out_dir/osr-source" rev-parse HEAD
cmake --build "$cmake_dir" --target osr-extract --config Release --parallel "${OSR_BUILD_JOBS:-2}"

extractor="$cmake_dir/osr-extract"
test -x "$extractor" || {
  echo "OSR CMake did not produce $extractor" >&2
  exit 1
}
install -m 755 "$extractor" ./osr-extract

# Fail before downloading a PBF if native runtime dependencies are missing.
ldd ./osr-extract | tee /tmp/osr-extractor-dependencies.txt
if grep -F 'not found' /tmp/osr-extractor-dependencies.txt; then
  exit 1
fi
sha256sum ./osr-extract
