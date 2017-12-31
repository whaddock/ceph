dstat --full --output /tmp/test1-everest-0-0-$timestamp.txt 1 360> /dev/null
/home/whaddock/ceph-build/examples/librados/librados_my_hello_world-0 -c /etc/ceph/ceph.conf > /tmp/test1-everest-0-0-p0-$timestamp.txt
/home/whaddock/ceph-build/examples/librados/librados_my_hello_world-1 -c /etc/ceph/ceph.conf > /tmp/test1-everest-0-0-p2-$timestamp.txt
