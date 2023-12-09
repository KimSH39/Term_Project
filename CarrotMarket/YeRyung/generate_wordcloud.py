import happybase
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import subprocess
import os

# HBase 연결 설정
connection = happybase.Connection('localhost')

# 사용할 테이블 목록 정의
table_names = ['emergencybell_news', 'bus_news', 'subway_news', 'cctv_news', 'securitylight_news', 'police_news']

# 각 테이블에 대해 WordCloud 생성 및 이미지 저장
for table_name in table_names:
    table = connection.table(table_name)

    scan_data = {}
    for key, data in table.scan():
        scan_data[key.decode("utf-8")] = int(data[b'cf:count'])

    wc = WordCloud(font_path="/home/maria_dev/sensibility/SUITE-Medium.ttf", width=800, height=400, background_color='white')
    cloud = wc.generate_from_frequencies(scan_data)

    plt.figure(figsize=(12, 10))
    plt.imshow(cloud, interpolation='bilinear')
    plt.axis('off')

    local_image_path = f'/home/maria_dev/wordcloud/wordcloud_{table_name}.png'
    if os.path.exists(local_image_path):
        os.remove(local_image_path)
    plt.savefig(local_image_path)

    hdfs_image_path = f'/user/maria_dev/wordcloud/wordcloud_{table_name}.png'
    subprocess.run(['hadoop', 'fs', '-copyFromLocal', '-f', local_image_path, hdfs_image_path])

# 연결 종료
connection.close()
