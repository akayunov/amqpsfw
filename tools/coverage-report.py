import subprocess
import os.path

# --cov-report html
# --cov-report xml
# --cov-report annotate
# --cov=myproj tests/
scr_dir = os.path.dirname(os.path.realpath(__file__))
subprocess.call([
    os.path.join(scr_dir, '../../venv/bin/py.test'),
    '--cov', os.path.join(scr_dir, '../lib'),
    '--cov-report', 'annotate',
    os.path.join(scr_dir, '../test')]
)
