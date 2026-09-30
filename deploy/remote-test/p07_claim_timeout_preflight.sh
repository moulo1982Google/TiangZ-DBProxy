#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 && $1 =~ ^p7ct_[a-z0-9_]{1,14}$ ]] || { echo 'Rejected timeout RunId' >&2; exit 2; }
for variable in ${!P07_@}; do
    echo "Rejected timeout override: $variable" >&2
    exit 2
done
