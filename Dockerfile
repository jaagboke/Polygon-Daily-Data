FROM apache/airflow:2.8.1

USER airflow
#Copy requirements.txt file
COPY requirements.txt /tmp/requirements.txt

#Install python packages as root
RUN pip install --no-cache-dir -r /tmp/requirements.txt

#Switch back to airflow user for safety
USER airflow