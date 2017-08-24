import subprocess
import os.path

# --cov-report html
# --cov-report xml
# --cov-report annotate
# --cov=myproj tests/
scr_dir = os.path.dirname(os.path.realpath(__file__))
tmp_dir = os.path.join(os.path.dirname(scr_dir), 'tmp')
subprocess.call([
    os.path.join(scr_dir, '../../venv/bin/py.test'),
    '--cov', os.path.join(scr_dir, '../lib'),
    '--cov-report', 'xml:' + tmp_dir + '/cov_xml',
    '--cov-report', 'html:' + tmp_dir + '/cov_html',
    '--cov-report', 'annotate:' + tmp_dir + '/cov_annotate',
    os.path.join(scr_dir, '../test')]
)
