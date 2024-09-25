import pymysql
import threading
import configparser
import time

def monitor_replication(db_config, poll_interval):
    """在只读副本上实时监控表中的新插入记录，并计算复制延迟。"""
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            # 初始化 last_checked_id 为当前表中最大的 id
            cursor.execute("SELECT IFNULL(MAX(id), 0) FROM replication_latency_test")
            result = cursor.fetchone()
            last_checked_id = result[0] if result else 0

            print(f'Starting to monitor the table for new entries from ID > {last_checked_id}...')
            while True:
                # 查询 ID 大于 last_checked_id 的新记录
                sql = """
                SELECT id, write_time, NOW(6) as read_time
                FROM replication_latency_test
                WHERE id > %s
                ORDER BY id ASC
                """
                cursor.execute(sql, (last_checked_id,))
                results = cursor.fetchall()
                if results:
                    for result in results:
                        record_id, write_time, read_time = result
                        latency = (read_time - write_time).total_seconds() * 1000  # 转换为毫秒
                        print(f'New Entry Detected:')
                        print(f'  ID         : {record_id}')
                        print(f'  Write Time : {write_time}')
                        print(f'  Read Time  : {read_time}')
                        print(f'  Replication Latency: {latency:.2f} ms\n')
                        # 更新 last_checked_id
                        last_checked_id = max(last_checked_id, record_id)
                else:
                    # 没有新记录时，等待一定的时间再查询
                    time.sleep(poll_interval)

    except KeyboardInterrupt:
        print('\nMonitoring stopped by user.')
    except Exception as e:
        print(f'Error occurred: {e}')
    finally:
        connection.close()

def main():
    # 读取配置文件
    config = configparser.ConfigParser()
    config.read('config.ini')

    # 获取只读副本数据库连接信息
    replica_db_config = {
        'host': config.get('REPLICA_DB', 'host'),
        'user': config.get('REPLICA_DB', 'user'),
        'password': config.get('REPLICA_DB', 'password'),
        'database': config.get('REPLICA_DB', 'database'),
        'port': config.getint('REPLICA_DB', 'port'),
        'autocommit': True
    }

    # 获取测试参数
    poll_interval = config.getfloat('TEST_SETTINGS', 'poll_interval')

    # 启动监控复制的进程
    monitor_replication(replica_db_config, poll_interval)

if __name__ == '__main__':
    main()

