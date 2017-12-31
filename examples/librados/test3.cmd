dstat --full --output /tmp/test3-`hostname`-$timestamp.txt 1 180> /dev/null
/usr/bin/sleep 5 && `pwd`/rados_aio_write_test 6000 8388608 test${first} stripe -c /etc/ceph/ceph.conf > /tmp/test3-`hostname`-${first}-$timestamp.txt
/usr/bin/sleep 5 && `pwd`/rados_aio_write_test 6000 8388608 test${second} stripe -c /etc/ceph/ceph.conf > /tmp/test3-`hostname`-${second}-$timestamp.txt
/usr/bin/sleep 5 && `pwd`/rados_aio_write_test 6000 8388608 test${third} stripe -c /etc/ceph/ceph.conf > /tmp/test3-`hostname`-${third}-$timestamp.txt
