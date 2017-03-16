#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 new-version" >&2
    exit 1
fi

sed "s/version='[^']*'/version='$1'/" -i setup.py
sed "s/__version__\s*=\s*'[^']*'/__version__ = '$1'/" -i flamenco_worker/__init__.py

git diff
echo
echo "Don't forget to tag and commit!"
