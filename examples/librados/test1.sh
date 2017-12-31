#!/bin/bash
# run a test on the everest cluster to measure rados performance
export timestamp=`date +%Y%m%d%H%M%S`
echo "starting test at $timestamp"
/usr/bin/parallel -j 36 < test1.cmd
echo -n "finished test at "
echo `date +%Y%m%d%H%M%S`
echo "sleeping 15 seconds"
sleep 15
