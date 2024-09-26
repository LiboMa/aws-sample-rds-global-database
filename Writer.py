import pymysql
import threading
import argparse
import configparser

def insert_record(thread_id, num_inserts, db_config):
    """在主实例上插入多条记录，并记录写入时间。"""
    try:
        connection = pymysql.connect(**db_config)
        for i in range(num_inserts):
            with connection.cursor() as cursor:
                sql = "INSERT INTO replication_latency_test (message, write_time) VALUES (%s, NOW(6))"
                message = f'Thread {thread_id} message {i+1}'
                cursor.execute(sql, (message,))
                insert_id = cursor.lastrowid
                connection.commit()
                print(f'Thread {thread_id}: Inserted record ID {insert_id}')
    except Exception as e:
        print(f'Thread {thread_id}: Error occurred - {e}')
    finally:
        connection.close()

def main(concurrency, num_inserts):
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

    threads = []
    for i in range(concurrency):
        t = threading.Thread(target=insert_record, args=(i+1, num_inserts, primary_db_config))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Concurrent Write Test Script')
    parser.add_argument('--concurrency', type=int, help='并发线程数')
    parser.add_argument('--num-inserts', type=int, help='每个线程的插入次数')
    args = parser.parse_args()

    # 如果未在命令行指定参数，则从配置文件读取
    config = configparser.ConfigParser()
    config.read('config.ini')

    concurrency = args.concurrency if args.concurrency else config.getint('TEST_SETTINGS', 'concurrency')
    num_inserts = args.num_inserts if args.num_inserts else config.getint('TEST_SETTINGS', 'num_inserts')

    main(concurrency, num_inserts)