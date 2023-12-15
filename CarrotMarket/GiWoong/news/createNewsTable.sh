#!/bin/bash
bash /home/maria_dev/news/getNewsAndCopyToHDFS.sh
wait
if [ $? -eq 0 ]; then
    echo "wget done"
fi
sleep 5
export PYSPARK_PYTHON=python3.6
spark-submit /home/maria_dev/wordcount/konlpy_hbase_csv_sysin.py /user/maria_dev/crawling/BusNews.csv bus_news >  /home/maria_dev/wordcountToHBASE_log
wait
if [ $? -eq 0 ]; then
    echo "bus_news done"
fi
sleep 5
spark-submit /home/maria_dev/wordcount/konlpy_hbase_csv_sysin.py /user/maria_dev/crawling/cctvNews.csv cctv_news >> /home/maria_dev/wordcountToHBASE_log
wait
if [ $? -eq 0 ]; then
    echo "cctv_news done"
fi
sleep 5
spark-submit /home/maria_dev/wordcount/konlpy_hbase_csv_sysin.py /user/maria_dev/crawling/emergencybellNews.csv emergencybell_news >> /home/maria_dev/wordcountToHBASE_log
wait 
if [ $? -eq 0 ]; then
    echo "emergencybell_news done"
fi
sleep 5
spark-submit /home/maria_dev/wordcount/konlpy_hbase_csv_sysin.py /user/maria_dev/crawling/PoliceNews.csv police_news >> /home/maria_dev/wordcountToHBASE_log
wait
if [ $? -eq 0 ]; then
    echo "police_news done"
fi
sleep 5
spark-submit /home/maria_dev/wordcount/konlpy_hbase_csv_sysin.py /user/maria_dev/crawling/SecuritylightNews.csv securitylight_news >> /home/maria_dev/wordcountToHBASE_log
wait 
if [ $? -eq 0 ]; then
    echo "securitylight_news done"
fi
sleep 5
spark-submit /home/maria_dev/wordcount/konlpy_hbase_csv_sysin.py /user/maria_dev/crawling/subwayNews.csv subway_news >> /home/maria_dev/wordcountToHBASE_log
wait 
if [ $? -eq 0 ]; then
    echo "subway_news done"
fi
sleep 5
python3.6 /home/maria_dev/generate_wordcloud.py
if [ $? -eq 0 ]; then
    echo "wordcloud done"
fi