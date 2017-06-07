#!/bin/sh

set -e
flake8 shelver
coverage erase
coverage run --source shelver -m py.test
coverage report --include='shelver/**' --omit='shelver/test/**'
