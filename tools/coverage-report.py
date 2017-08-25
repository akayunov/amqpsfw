import subprocess
import os.path

scr_dir = os.path.dirname(os.path.realpath(__file__))
tmp_dir = os.path.join(os.path.dirname(scr_dir), 'tmp')
subprocess.call([
    os.path.join(scr_dir, '../../venv/bin/pytest'),
    '--cov', os.path.join(scr_dir, '../lib'),
    '--cov-report', 'html:' + tmp_dir + '/cov_html',
    os.path.join(scr_dir, '../test')]
)
