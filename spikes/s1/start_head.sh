#!/bin/bash
# Runs inside the s1ray container.
export PYTHONPATH=/home/ray/s1/code RAY_enable_autoscaler_v2=1 RAY_CLOUD_INSTANCE_ID=head RAY_NODE_TYPE_NAME=head
/home/ray/anaconda3/bin/ray start --head --node-ip-address=172.31.17.228 --port=6379 --num-cpus=0 \
  --autoscaling-config=/home/ray/s1/code/s1.yaml --dashboard-host=127.0.0.1
