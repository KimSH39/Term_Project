import happybase
from wordcloud import WordCloud # pip3.6 install wordcloud==1.8.0
import matplotlib.pyplot as plt

connection = happybase.Connection('localhost')

table_name = 'emergencybell_news'
table = connection.table(table_name)

scan_data = {}
for key, data in table.scan():
    # key는 row key, data는 해당 row의 데이터
    scan_data[key.decode("utf-8")] = int(data[b'cf:count'])

wc = WordCloud(font_path="/home/maria_dev/sensibility/SUITE-Medium.ttf", width=800, height=400, background_color='white')
cloud = wc.generate_from_frequencies(scan_data)

plt.figure(figsize=(12, 10))
plt.imshow(cloud, interpolation='bilinear')  # cloud를 사용하여 이미지 표시
plt.axis('off')
plt.savefig('hdfs:///user/maria_dev/wordcloud/wordcloud_emergencybell.png')  # 이미지로 저장
plt.show()

connection.close()
