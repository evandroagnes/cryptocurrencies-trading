#!/bin/bash

#crontab examples
# m h  dom mon dow   command
# Restart daily at 0:00
#0 0 * * * /home/user/trading.sh restart /home/user/cryptocurrencies-trading >> /home/user/trading.log
# Verify if the process is still running for each 30 minutes, if not start it.
#30 * * * * /home/user/trading.sh start /home/user/cryptocurrencies-trading >> /home/user/trading.log

OP=$1
TRADING_DIR=$2

#Find the Process ID for trade running instance
find_trading_process_id() {
        PID=`ps -ef | grep trade.py | grep -v grep | awk '{print $2}'`
}

#Start trading
start() {
        echo "Starting process..."
        find_trading_process_id
        if [[ "" ==  "$PID" ]]; then
                cd $TRADING_DIR
                nohup python3 -u $TRADING_DIR/trade.py >> $TRADING_DIR/output.log &
        else
                echo "Trading process is already running..." $PID
        fi
}

#Stop process
stop() {
        find_trading_process_id
        if [[ "" !=  "$PID" ]]; then
                echo "Stopping " $PID
                kill -2 $PID
                sleep 15
        fi

        find_trading_process_id
        if [[ "" !=  "$PID" ]]; then
                echo "Killing " $PID
                kill -9 $PID
                sleep 15
        fi
}

if [[ "$OP" == "start" ]]; then
        start
elif [[ "$OP" == "stop" ]]; then
        stop
elif [[ "$OP" == "restart" ]]; then
        stop
        sleep 10
        start
else
        echo "Usage:"
        echo "trading.sh <action> <trading directory>"
        echo 'where:'
        echo "action = start, stop or restart"
fi

exit 0