#!/bin/sh

flake8 shelver || :
coverage erase && \
coverage run --source shelver -m py.test && \
coverage report --include='shelver*' --omit='*test'
