# This file is supposed to run inside Blender:
# blender thefile.blend \
#     --python /path/to/exr_sequence_to_jpeg.py \
#     -- --exr-pattern /path/to/exr/files/prefix-*.exr

import argparse
import pathlib
import re
import sys

import bpy

# Find the EXR files to process.
dashdash_index = sys.argv.index('--')
parser = argparse.ArgumentParser()
parser.add_argument('--exr-glob')
parser.add_argument('--output-pattern')
cli_args, _ = parser.parse_known_args(sys.argv[dashdash_index + 1:])

exr_glob = pathlib.Path(cli_args.exr_glob)
imgdir = exr_glob.parent
exr_files = sorted(imgdir.glob(exr_glob.name))

if not exr_files:
    raise ValueError(f'No files found for pattern {exr_glob}')

# Create a copy of the scene without data, so we can fill the sequence editor
# with an image sequence.
bpy.ops.scene.new(type='EMPTY')

scene = bpy.context.scene
se = scene.sequence_editor_create()

# Place files at the correct frame, based on their filename.
# This makes the rendering consistent w.r.t. gaps in the frames.
# This assumes the files are named '000020.exr' etc.
min_frame = float('inf')
max_frame = float('-inf')

# Interpret the last continuous string of digits as frame number.
frame_nr_re = re.compile(r'[0-9]+$')
print(f'Loading {len(exr_files)} EXR files:')
for file in exr_files:
    match = frame_nr_re.search(file.stem)
    if not match:
        raise ValueError(f'Unable to find frame number in filename {file.name}')
    frame_num = int(match.group(), 10)
    min_frame = min(min_frame, frame_num)
    max_frame = max(max_frame, frame_num)
    print(f'   - {file} -> frame {frame_num}')
    se.sequences.new_image(file.name, str(file), 1, frame_num)
print(f'Found files for frame range {min_frame}-{max_frame}')
print()
sys.stdout.flush()

scene.frame_start = min_frame
scene.frame_end = max_frame

render = scene.render
render.use_sequencer = True
render.filepath = str(imgdir / cli_args.output_pattern)
render.image_settings.file_format = 'JPEG'
render.image_settings.quality = 90
render.use_overwrite = True  # overwrite lesser quality previews

bpy.ops.render.render(animation=True, use_viewport=False)
bpy.ops.wm.quit_blender()
