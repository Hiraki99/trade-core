#!/bin/bash
# $app1 = "runDeposit.py"
# $app2 = "runGateway.py"
# $app3 = "runTrade.py"
test=`ps aux | grep "runDeposit\|runGateway\|runTrade\|runPreTrade\|runKafka" | grep -v grep -c`
if [ $test == 0 ]; then
        #pip install -r requirements.txt
        python runDeposit.py --env=deposit >> logs-deposit.log  &
        python runGateway.py --env=gateway >> logs-gateway.log  &
        python runTrade.py --env=trade >> logs-trade.log  &
        python runPreTrade.py --env=pre_trade >> logs-pre-trade.log  &
        python runKafka.py --env=kafka >> logs-kafka.log  &
        echo "[INFO] Service is starting"
        exit
else
        echo "[WARN] Service is already running"
        exit
fi