#!/bin/bash

test=`ps aux | grep "runDeposit\|runGateway\|runTrade" | grep -v grep -c`
if [ $test == 0 ]; then
        #pip install -r requirements.txt
        python runDeposit.py --env=deposit   &
        python runGateway.py --env=gateway   &
        python runTrade.py --env=trade   &
        echo "[INFO] Service is starting"
        exit
else
        echo "[WARN] Service is already running"
        exit
fi