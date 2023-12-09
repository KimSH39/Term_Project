from pyspark.sql import SparkSession
from konlpy.tag import Okt
import happybase
import sys
# SparkSession 생성
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# CSV 파일을 DataFrame으로 읽기
file_path = sys.argv[1]  # CSV 파일 경로 설정
df = spark.read.option("header", "true").option("multiLine", "true").option("quote", "\"").option("escape", "\"").csv(file_path)
# 확인용
df.show(1)
# '기사 제목'과 '기사 내용' 컬럼 선택
articles_df = df

# Konlpy를 이용한 불용어 처리를 위한 함수 정의
def remove_stopwords(text):
    if text == None:
        return []
    okt = Okt()
    words = okt.nouns(text)
    clean_tokens = [(word,) for word in words]
    return clean_tokens

# '기사 제목'을 row key로 하여 단어 빈도 계산 및 HBase에 데이터 저장
connection = happybase.Connection('localhost', port=9090)  # HBase 호스트와 포트 입력
table = connection.table(sys.argv[2])  # HBase 테이블 이름 입력

for row in articles_df.collect():
    # print(row)
    title = row[0]
    content = row[1]
    # 기사 내용의 불용어 제거 및 단어별로 분할하여 RDD로 변환
    words_rdd = spark.sparkContext.parallelize(remove_stopwords(content))

    # 각 단어의 출현 횟수 계산
    word_counts = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).collect()

    # HBase에 데이터 저장 (기사 제목을 row key로 사용)
    # for word, count in word_counts:
    #     table.put(
    #         str(title).encode(),  # 기사 제목을 row key로 변환하여 입력
    #         {b'cf:word': str(word[0]).encode(), b'cf:count': str(count).encode()}
    #     )
    for word, count in word_counts:
        table.put(
            word[0].encode('utf-8'),
            {'cf:count': str(count)}
        )

# 연결 종료
connection.close()

# SparkSession 종료
spark.stop()
