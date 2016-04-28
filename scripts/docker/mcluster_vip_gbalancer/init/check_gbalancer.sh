#!/bin/bash
if [ $1 -eq 3306 ]
then
/usr/bin/nohup /usr/bin/gbalancer --config=/etc/gbalancer/3306configuration.json -daemon  >> /nohup.out  2>&1 &
fi
if [ $1 -eq 8888 ]
then
/usr/bin/nohup /usr/bin/gbalancer --config=/etc/gbalancer/8888configuration.json -daemon  >> /nohup.out  2>&1 &
fi
