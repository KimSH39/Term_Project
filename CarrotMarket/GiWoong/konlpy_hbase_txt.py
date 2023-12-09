from konlpy.tag import Okt
from pyspark import SparkContext
from pyspark.sql import SparkSession
import happybase

connection = happybase.Connection('localhost')

# connect to hbase
table_name = "word_table"
table = connection.table(table_name)


sc = SparkContext(appName="KoNLPyExample")
spark = SparkSession(sc)

file_path = "/home/maria_dev/news/news_police.csv" 
df = spark.read.option("header", "true").csv(file_path)

def remove_stopwords(text):
    okt = Okt()
    words = okt.nouns(text)
    return [(word,) for word in words]
# df = spark.createDataFrame(word_list, ["word"])

words_rdd = df.rdd.flatMap(lambda row: remove_stopwords(row[1]))
word_counts = words_rdd.map(lambda word: (word['word'], 1)).reduceByKey(lambda acc, new: acc + new).collect()

for word, count in word_counts:
    word_str = word
    table.put(
        word_str.encode("utf-8"), 
        {'cf:count': str(count)}
    )
