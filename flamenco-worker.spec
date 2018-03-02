# -*- mode: python -*-

# This definition is used by PyInstaller to build ready-to-run bundles.

from flamenco_worker import __version__

block_cipher = None


a = Analysis(['flamenco-worker.py'],
             pathex=['./flamenco-worker-python'],
             binaries=[],
             datas=[('flamenco-worker.cfg', '.'),
                    ('README.md', '.'),
                    ('CHANGELOG.md', '.'),
                    ('LICENSE.txt', '.'),
                    ('flamenco_worker/merge-exr.blend', 'flamenco_worker')],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='flamenco-worker',
          debug=False,
          strip=False,
          upx=True,
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               name='flamenco-worker-%s' % __version__)
