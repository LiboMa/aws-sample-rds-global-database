import pymysql
import datetime

# 配置主实例的连接信息
PRIMARY_DB_CONFIG = {
    'host': 'globa-db-cluster-1.cluster-cryagg0uu1p6.sa-east-1.rds.amazonaws.com',    # 替换为您的主实例终端节点
    'user': 'admin',            # 替换为您的用户名
    'password': 'mlb308119',          # 替换为您的密码
    'database': 'replication_test',
    'port': 3306                 # MySQL默认端口
}

def insert_record():
    """在主实例上插入一条记录，并记录写入时间。"""
    connection = pymysql.connect(**PRIMARY_DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            write_time = datetime.datetime.utcnow()
            sql = "INSERT INTO replication_latency_test (message, write_time) VALUES (%s, %s)"
            cursor.execute(sql, ('Test Message', write_time))
            insert_id = cursor.lastrowid
            connection.commit()
            print(f'Inserted record ID {insert_id} at {write_time}')
    finally:
        connection.close()

if __name__ == '__main__':
    insert_record()

