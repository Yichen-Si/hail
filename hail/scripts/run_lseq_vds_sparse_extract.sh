#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

SCALA_VERSION="${SCALA_VERSION:-2.12}"

classpath="$(
  SCALA_VERSION="$SCALA_VERSION" bash mill show "hail[$SCALA_VERSION].compileClasspath" 2>/dev/null \
    | sed -n \
      -e 's/.*qref:[^:]*:[^:]*:\([^"]*\)".*/\1/p' \
      -e 's/.*ref:[^:]*:[^:]*:\([^"]*\)".*/\1/p' \
    | paste -sd: -
)"

java -cp "out/hail/$SCALA_VERSION/compile.dest/classes:hail/resources:$classpath" \
  is.hail.tools.LSeqVdsSparseExtract "$@"
