#!/bin/bash

for ((j=0;j<500;j++));do
{
    /usr/local/bin/python3 /Users/zoe/Workspaces/github/tidb/tidb-thread.py -f ${j}
    wait
    /usr/local/bin/python3 /Users/zoe/Workspaces/github/tidb/tidb-db.py 127.0.0.1
}
done
