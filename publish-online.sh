#!/bin/bash -e

FLAMENCO_VERSION="2.0.7"
echo "Uploading Flamenco Worker $FLAMENCO_VERSION to flamenco.io"

cd dist
rsync \
    flamenco-worker-${FLAMENCO_VERSION}.zip \
    flamenco-worker-${FLAMENCO_VERSION}.sha256 \
    armadillica@flamenco.io:flamenco.io/download/ -va

echo "Done!"
