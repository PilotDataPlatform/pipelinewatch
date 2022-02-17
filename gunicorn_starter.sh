#!/bin/sh
# ps -ef | grep python | cut -c 9-15| xargs kill -s 9
python3 worker_air_flow_watch.py &
python3 worker_k8s_job_watch.py &
