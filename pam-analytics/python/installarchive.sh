#!/bin/bash

if [ -z "$1" ]
  then
    echo "No version supplied"
    exit 1
fi

rm -rf pam_analytics.egg-info
rm -rf dist/* &&
rm VERSION
echo $1 > VERSION
git tag -a $1 -m "build $1"
python -m compileall -f . &&
python setup.py sdist
curl -F package=@dist/pam-analytics-$1.tar.gz https://o7L8pPPHY7cvDroVBd1h@push.fury.io/autogrid/
rm -rf pam_analytics.egg-info
