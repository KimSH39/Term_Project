from pyspark.sql import SparkSession
from konlpy.tag import Okt
import happybase
import sys

dependent_nouns = [
    "것", "겸", "김", "길", "나름", "나위", "동안", "대로", "따름", "때문", "데", "듯", "리", "마련", "만", "만큼",
    "망정", "모양", "바", "바람", "뻔", "뿐", "법", "수", "성", "셈", "십상", "일쑤", "적", "줄", "지", "지경", "중",
    "참", "채", "체", "척", "탓", "터", "턱", "통", "판", "편", "한", "을", "를", "이", "가", "등", "곳", "말", "내", 
    "며", "과", "스", "또", "몇", "더"
]

def remove_stopwords(text):
    if text is None:
        return []
    okt = Okt()
    words = okt.nouns(text)
    clean_tokens = [(word,) for word in words if word not in dependent_nouns]
    return clean_tokens

spark = SparkSession.builder.appName("WordCount").config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g").getOrCreate()

file_path = sys.argv[1]
table_name = sys.argv[2]

# Read the CSV file and repartition the DataFrame
articles_df = spark.read.option("header", "true").option("multiLine", "true").option("quote", "\"").option("escape", "\"").csv(file_path)
articles_df = articles_df.repartition(30)


# Process the DataFrame and perform word count operations
word_counts_rdd = articles_df.rdd.flatMap(lambda x: remove_stopwords(x[1])).map(lambda x: (x, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[0][0].encode('utf-8'), str(x[1])))

table_columns = {
    'cf:count': dict()
}

connection = happybase.Connection('localhost', port=9090)
if table_name.encode() in connection.tables():
    connection.disable_table(table_name)
    connection.delete_table(table_name)

connection.create_table(table_name, table_columns)
table = connection.table(table_name)
connection.close()

for word, count in word_counts_rdd.collect():
    connection = happybase.Connection('localhost', port=9090)
    table = connection.table(table_name)
    data = {b'cf:count': count}
    table.put(word, data)
    connection.close()
spark.stop()
