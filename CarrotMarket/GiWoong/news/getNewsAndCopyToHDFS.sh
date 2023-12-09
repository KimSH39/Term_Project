#!/bin/bash
wget https://raw.githubusercontent.com/2023-MJU-BDP2/Term_Project/main/CarrotMarket/JiHyeon/AutoCrawling/BusNews.csv
wget https://raw.githubusercontent.com/2023-MJU-BDP2/Term_Project/main/CarrotMarket/JiHyeon/AutoCrawling/PoliceNews.csv
wget https://raw.githubusercontent.com/2023-MJU-BDP2/Term_Project/main/CarrotMarket/JiHyeon/AutoCrawling/SecuritylightNews.csv
wget https://raw.githubusercontent.com/2023-MJU-BDP2/Term_Project/main/CarrotMarket/JiHyeon/AutoCrawling/cctvNews.csv
wget https://raw.githubusercontent.com/2023-MJU-BDP2/Term_Project/main/CarrotMarket/JiHyeon/AutoCrawling/emergencybellNews.csv
wget https://raw.githubusercontent.com/2023-MJU-BDP2/Term_Project/main/CarrotMarket/JiHyeon/AutoCrawling/subwayNews.csv
hadoop fs -copyFromLocal /home/maria_dev/news/* /user/maria_dev/news/
