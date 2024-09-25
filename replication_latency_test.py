import pymysql
import threading
import configparser
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def insert_records(thread_id, num_inserts, db_config, result_queue):
    """在主实例上插入多条记录，并记录写入时间。"""
    try:
        connection = pymysql.connect(**db_config)
        insert_ids = []
        for i in range(num_inserts):
            with connection.cursor() as cursor:
                sql = "INSERT INTO replication_latency_test (message, write_time) VALUES (%s, NOW(6))"
                message = f'Thread {thread_id} message {i+1}'
                cursor.execute(sql, (message,))
                insert_id = cursor.lastrowid
                connection.commit()
                print(f'Thread {thread_id}: Inserted record ID {insert_id}')
                insert_ids.append(insert_id)
        # 将插入的ID列表添加到结果队列中
        result_queue.extend(insert_ids)
    except Exception as e:
        print(f'Thread {thread_id}: Error occurred - {e}')
    finally:
        connection.close()

def monitor_replication(db_config, insert_ids, poll_interval):
    """在只读副本上监控插入的记录，计算复制延迟。"""
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            remaining_ids = set(insert_ids)
            while remaining_ids:
                sql = """
                SELECT id, write_time, NOW(6) as read_time
                FROM replication_latency_test
                WHERE id IN (%s)
                """
                format_strings = ','.join(['%s'] * len(remaining_ids))
                cursor.execute(sql % format_strings, tuple(remaining_ids))
                results = cursor.fetchall()
                if results:
                    for result in results:
                        record_id, write_time, read_time = result
                        latency = (read_time - write_time).total_seconds() * 1000  # 转换为毫秒
                        print(f'Record ID {record_id}:')
                        print(f'  Write Time (DB): {write_time}')
                        print(f'  Read Time  (DB): {read_time}')
                        print(f'  Replication Latency: {latency:.2f} ms\n')
                        remaining_ids.discard(record_id)
                else:
                    time.sleep(poll_interval)  # 等待后重试
    finally:
        connection.close()

def main():
    # 读取配置文件
    config = configparser.ConfigParser()
    config.read('config.ini')

    # 获取主数据库连接信息
    primary_db_config = {
        'host': config.get('PRIMARY_DB', 'host'),
        'user': config.get('PRIMARY_DB', 'user'),
        'password': config.get('PRIMARY_DB', 'password'),
        'database': config.get('PRIMARY_DB', 'database'),
        'port': config.getint('PRIMARY_DB', 'port')
    }

    # 获取只读副本数据库连接信息
    replica_db_config = {
        'host': config.get('REPLICA_DB', 'host'),
        'user': config.get('REPLICA_DB', 'user'),
        'password': config.get('REPLICA_DB', 'password'),
        'database': config.get('REPLICA_DB', 'database'),
        'port': config.getint('REPLICA_DB', 'port')
    }

    # 获取测试参数
    concurrency = config.getint('TEST_SETTINGS', 'concurrency')
    num_inserts = config.getint('TEST_SETTINGS', 'num_inserts')
    poll_interval = config.getfloat('TEST_SETTINGS', 'poll_interval')

    # 存储所有插入的记录ID
    all_insert_ids = []

    # 创建线程池执行器
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        # 使用线程安全的列表来收集插入的ID
        result_queue = []
        for i in range(concurrency):
            futures.append(executor.submit(insert_records, i+1, num_inserts, primary_db_config, result_queue))

        # 等待所有插入操作完成
        for future in as_completed(futures):
            pass

    # 等待所有记录的ID被收集
    while len(result_queue) < concurrency * num_inserts:
        time.sleep(0.1)

    all_insert_ids = result_queue.copy()

    print("\nAll records inserted. Starting replication monitoring...\n")

    # 启动监控复制的线程
    monitor_replication(replica_db_config, all_insert_ids, poll_interval)

if __name__ == '__main__':
    main()

