import logging
import os
import pathlib
import platform
import shutil
import unittest


class AbstractWorkerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)-15s %(levelname)8s %(name)s %(message)s',
        )

    def find_blender_cmd(self) -> str:
        if platform.system() == 'Windows':
            blender = 'blender.exe'
        else:
            blender = 'blender'

        found = shutil.which(blender)
        if found is None:
            self.fail(f'Unable to find {blender!r} executable on $PATH')

        return pathlib.Path(found).as_posix()
