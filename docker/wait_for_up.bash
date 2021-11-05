#!/bin/sh
HOST=$1
PORT=$2
TIMEOUT=${3:-3}
[[ -z $HOST ]] && exit 1
[[ -z $PORT ]] && exit 1

for run in $(seq 1 $TIMEOUT); do
    nc -z $HOST $PORT
    [[ $? -eq 0 ]] && exit 0
    sleep 15
done

nc -z $HOST $PORT
[[ $? -eq 0 ]] && exit 0

echo "$HOST:$PORT was not open after $TIMEOUT connection attempts"
exit 1
