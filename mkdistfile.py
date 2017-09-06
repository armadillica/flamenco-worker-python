#!/usr/bin/env python3

"""Builds a distributable file."""

import platform
import re
import shutil
import sys
from pathlib import Path

from PyInstaller.__main__ import run
from flamenco_worker import __version__


def create_tar(prefix: str, dist_dir: Path):
    """Creates a gzipped tarball.

    Removes the directory that was tarballed.
    """
    import tarfile

    tar_fname = '%s-%s.tar.gz' % (prefix, platform.system().lower())
    tar_path = dist_dir / tar_fname
    to_tar = str(dist_dir / prefix)

    with tarfile.open(str(tar_path), 'w:gz') as tar:
        tar.add(to_tar, prefix, recursive=True)

    shutil.rmtree(to_tar)
    print('Created', tar_path)


def create_zip(prefix: str, dist_dir: Path):
    """Creates a zip file.

    Removes the directory that was zipped.
    """
    import zipfile

    zip_fname = '%s-%s.zip' % (prefix, platform.system().lower())
    zip_path = dist_dir / zip_fname
    to_zip = dist_dir / prefix

    with zipfile.ZipFile(str(zip_path), 'w', zipfile.ZIP_DEFLATED) as zfile:
        for fpath in to_zip.glob('**/*'):
            zfile.write(str(fpath), str(fpath.relative_to(to_zip)))

    shutil.rmtree(str(to_zip))
    print('Created', zip_path)


def main():
    print('Building installation directory')
    sys.argv = ['pyinstaller', 'flamenco-worker.spec', '--log-level', 'WARN']
    run()

    prefix = 'flamenco-worker-%s' % __version__
    dist_dir = Path('dist').absolute()

    print('Creating archive')
    if platform.system().lower() == 'windows':
        create_zip(prefix, dist_dir)
    else:
        create_tar(prefix, dist_dir)


if __name__ == '__main__':
    main()
