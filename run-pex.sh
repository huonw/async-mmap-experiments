#!/usr/bin/env sh
PEX_SCRIPT=pex3 pex lock sync --style=universal --indent=2 -r=requirements.txt  --lock=pex.lock --venv=.venv -- "$@"
