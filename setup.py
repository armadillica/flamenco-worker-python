#!/usr/bin/env python

from pathlib import Path
import collections
import hashlib
import setuptools
import sys
import zipfile

from distutils.cmd import Command
from distutils.errors import DistutilsOptionError
from distutils import dir_util, log

sys.dont_write_bytecode = True


# noinspection PyAttributeOutsideInit
class ZipCommand(Command):
    """Ensures that 'setup.py dist' creates a zip file with a wheel and other useful stuff."""

    description = "create a zip with a wheel and other useful files"
    user_options = [
        ('dist-dir=', 'd',
         "directory to put the archive in "
         "[default: dist]"),
    ]

    def initialize_options(self):
        self.dist_dir = None

    def finalize_options(self):
        if self.dist_dir is None:
            self.dist_dir = "dist"

    def run(self):
        self.run_command('bdist_wheel')
        if not self.distribution.dist_files:
            msg = "No dist file created, even though we ran 'bdist_wheel' ourselves."
            raise DistutilsOptionError(msg)

        base_dir = Path(self.distribution.get_fullname())
        zip_base = Path(self.dist_dir) / base_dir
        zip_name = zip_base.with_name(zip_base.name + '.zip')

        log.info('Creating ZIP file %s', zip_name)

        with zipfile.ZipFile(str(zip_name), mode='w') as archive:
            def add_to_root(fname: Path):
                log.info('    adding %s', fname.name)
                archive.write(str(fname), fname.name)

            for command, pyversion, filename in self.distribution.dist_files:
                add_to_root(Path(filename))

            add_to_root(Path('flamenco-worker.cfg'))
            add_to_root(Path('LICENSE.txt'))
            add_to_root(Path('README.md'))
            add_to_root(Path('CHANGELOG.md'))
            add_to_root(Path('flamenco_worker/merge-exr.blend'))

            paths = collections.deque([Path('system-integration')])
            while paths:
                this_path = paths.popleft()
                if this_path.is_dir():
                    paths.extend(this_path.iterdir())
                    continue

                log.info('    adding %s', this_path)
                archive.write(str(this_path), str(this_path))

        # Compute SHA256 checksum of the produced zip file.
        hasher = hashlib.sha256()
        blocksize = 65536
        with zip_name.open(mode='rb') as infile:
            buf = infile.read(blocksize)
            while len(buf) > 0:
                hasher.update(buf)
                buf = infile.read(blocksize)
        checksum_path = zip_name.with_suffix('.sha256')
        log.info('Writing SHA256 checksum to %s', checksum_path)
        with checksum_path.open(mode='w') as shafile:
            print('%s  %s' % (hasher.hexdigest(), zip_name.name), file=shafile)


if __name__ == '__main__':
    setuptools.setup(
        cmdclass={'zip': ZipCommand},
        name='flamenco-worker',
        version='2.2-dev9',
        description='Flamenco Worker implementation',
        author='Sybren A. Stüvel',
        author_email='sybren@blender.studio',
        packages=setuptools.find_packages(),
        data_files=[('flamenco_worker', ['README.md', 'LICENSE.txt', 'CHANGELOG.md'])],
        license='GPL',
        classifiers=[
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.5',
        ],
        package_data={'flamenco_worker': ['merge-exr.blend']},
        install_requires=[
            'attrs >=16.3.0',
            'requests>=2.12.4',
        ],
        entry_points={'console_scripts': [
            'flamenco-worker = flamenco_worker.cli:main',
        ]},
        zip_safe=False,  # due to the bundled merge-exr.blend file.
    )
