#!/bin/bash -e

FLAMENCO_VERSION="2.3-dev8"

cd dist

# First check that all files are there
MISSING=0
for PLAT in linux windows darwin; do
    PREFIX=flamenco-worker-${FLAMENCO_VERSION}-${PLAT}

    if [ -e ${PREFIX}.zip -o -e ${PREFIX}.tar.gz ]; then
        continue
    fi

    echo "Build for platform ${PLAT} incomplete, no such file ${PREFIX}.{zip,tar.gz}"
    MISSING=1
done

if [ $MISSING == "1" ]; then
    exit 2
fi

if [ -e flamenco-worker-${FLAMENCO_VERSION}.sha256 ]; then
    echo "Checking pre-existing SHA256 sums"
    sha256sum -c flamenco-worker-${FLAMENCO_VERSION}.sha256
    echo
fi

sha256sum flamenco-worker-${FLAMENCO_VERSION}-*.{zip,tar.gz} > flamenco-worker-${FLAMENCO_VERSION}.sha256

echo "Uploading Flamenco Worker $FLAMENCO_VERSION to flamenco.io"
rsync \
    flamenco-worker-${FLAMENCO_VERSION}-*.{zip,tar.gz} \
    flamenco-worker-${FLAMENCO_VERSION}.sha256 \
    armadillica@flamenco.io:flamenco.io/download/ -va

echo "Done!"
