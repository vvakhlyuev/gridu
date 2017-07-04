#!/bin/bash
flume-ng agent --conf /etc/flume-ng/conf -f /etc/flume-ng/conf/flume.conf \
        --name agent1 \
        -Dflume.root.logger=INFO,console \
        -Dorg.apache.flume.log.printconfig=true
#        -Dorg.apache.flume.log.rawdata=true