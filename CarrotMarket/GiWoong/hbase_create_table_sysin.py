import happybase
import sys

# HBase에 연결
connection = happybase.Connection('localhost')

# 테이블 생성
table_name = str(sys.argv[1])
connection.create_table(
    table_name,
    {'cf': dict()}
)
