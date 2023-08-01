import subprocess
from time import sleep

subprocess.Popen(['prefect', 'config', 'set', 'PREFECT_UI_API_URL="http://localhost:4200/api"'])
subprocess.Popen(['prefect', 'server', 'start', '--host', '0.0.0.0'])
sleep(5)
subprocess.Popen(['python', 'create_blocks.py'])
subprocess.Popen(['prefect', 'deploy', '-n', 'etl'])
sleep(5)
subprocess.check_output(['prefect', 'work-pool', 'create', '--type', 'process', 'etl-pool'])
sleep(5)
subprocess.Popen(['prefect', 'worker', 'start', '--pool', 'etl-pool'], shell=False)
