import happybase

# HBase에 연결
connection = happybase.Connection('localhost', port=9090)
# 테이블명 지정
table_name = 'emergencybell_news'
# 연결된 테이블 확인
table = connection.table(table_name)
# 전체 데이터 스캔 및 가져오기
scan_data = {}
for key, data in table.scan():
    # key는 row key, data는 해당 row의 데이터
    scan_data[key.decode("utf-8")] = int(data[b'cf:count'])
# 가져온 전체 데이터에서 가장 count 수가 높은 상위 10개 출력
print(sorted(scan_data.items(), key=lambda x: x[1], reverse=True)[:10]) 

# 연결 종료
connection.close()


