#!/bin/bash
kill -9 `ps -ef | grep sqes_multiprocessing.py | grep -v grep | awk '{print $2}'`