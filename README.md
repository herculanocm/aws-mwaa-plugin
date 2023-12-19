# AWS MWAA plugins
Customizing hooks, operators and sensors.

First generate wheel file for install with
```
virtualenv --python="/usr/bin/python3.10" "./venv"
pip install -r requeriments.txt
```

# PIP Deps
pip install "apache-airflow[postgres,amazon,google,jdbc,slack,ssh]==2.6.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-3.10.txt"