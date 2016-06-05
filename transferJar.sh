#!/usr/bin/env bash

gradle clean build
scp -P 51800 -r /Users/denis/Documents/Java/Hadoop/first/build/libs/spark_7_4-1.0-SNAPSHOT.jar s19421@remote.vdi.mipt.ru:~/Task1
