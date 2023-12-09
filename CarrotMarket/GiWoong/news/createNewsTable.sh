#!/bin/bash
python3.6 ~/wordcount/hbase_create_table_sysin.py bus_news
python3.6 ~/wordcount/hbase_create_table_sysin.py cctv_news
python3.6 ~/wordcount/hbase_create_table_sysin.py emergencybell_news
python3.6 ~/wordcount/hbase_create_table_sysin.py police_news       
python3.6 ~/wordcount/hbase_create_table_sysin.py securitylight_news
python3.6 ~/wordcount/hbase_create_table_sysin.py subway_news 