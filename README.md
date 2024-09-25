### For testing the global forwarder and replica on RDS global database

0. create schema

```sql
CREATE DATABASE IF NOT EXISTS replication_test;
USE replication_test;

CREATE TABLE IF NOT EXISTS replication_latency_test (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    message VARCHAR(255),
    write_time DATETIME(6),
    PRIMARY KEY (id)
);
```


1. config.ini
```bash
cat >config.ini<<EOF
[PRIMARY_DB]
 host = globa-db-cluster-1.cluster-cryagg0uu1p6.sa-east-1.rds.amazonaws.com
 user = admin
 password = PASSWD 
 database = replication_test
 port = 3306

 [REPLICA_DB]
 ;host = main-site-db-instance-1.cbo6yikg4rug.eu-west-3.rds.amazonaws.com
 host = globa-db-cluster-1.cluster-ro-cryagg0uu1p6.sa-east-1.rds.amazonaws.com
 user = admin
 password = PASSWD 
 database = replication_test
 port = 3306

 [TEST_SETTINGS]
 concurrency = 1
 num_inserts = 10
 poll_interval = 0.1
EOF

```

2. run Writer to insert recoard

```bash
python Writer.py
```

3. read recoard

```bash
python monitor_replication.py

```
