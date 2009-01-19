#!/bin/sh

if [ "X$1" = "X" ]; then
    echo "Missing label to search" 1>&2
    exit 1;
fi

label="$1";
device=`/sbin/findfs LABEL=$label`
if [ "$?" = 0 ]; then
    echo $device;
else
    exit 2;
fi

exit 0
