#!/usr/bin/env bash

SRCDIR="s3://dig-analysis-data/out/metaanalysis/plots/"
OUTDIR="s3://dig-bio-index/plots/"

aws s3 sync "${SRCDIR}" "${OUTDIR}"
