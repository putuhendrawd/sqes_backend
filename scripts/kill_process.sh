#!/bin/bash
kill -9 `ps -ef | grep sqes_v3_multiprocessing.py | grep -v grep | awk '{print $2}'`