import subprocess
import os.path
import sys
import pstats

scr_dir = os.path.dirname(os.path.realpath(__file__))
tmp_dir = os.path.join(os.path.dirname(scr_dir), 'tmp')

# python -m cProfile [-o output_file] [-s sort_order] myscript.py

subprocess.call([
    os.path.join(scr_dir, '../../venv/bin/python'),
    '-m', 'cProfile',
    #'-s', '',
    '-o',  tmp_dir + '/' + os.path.basename(sys.argv[1]) + '_profile',
    sys.argv[1]]
)
p_stats = pstats.Stats(tmp_dir + '/' + os.path.basename(sys.argv[1]) + '_profile')
p_stats.sort_stats('tottime')
p_stats.print_stats()