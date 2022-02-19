'''
python3 galera/galera-db.py 127.0.0.1
'''

import os,sys
import pymysql


server = sys.argv[1]
key = 20

connect = pymysql.connect(host=server, port=4000, user="root", password="")
# Disable Auto-Commit
connect.autocommit = False

cursor = connect.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS tidb;")
cursor.execute("DROP TABLE IF EXISTS tidb.variables;")
cursor.execute("CREATE TABLE IF NOT EXISTS tidb.variables (var BIGINT(64) UNSIGNED NOT NULL PRIMARY KEY, val BIGINT(64) UNSIGNED NOT NULL);")

for i in range(0, key):
    cursor.execute("INSERT INTO tidb.variables (var, val) values (%d, 0);" % i)
# cursor.execute("SELECT * from tidb.variables;")
# print(cursor.fetchall())

connect.commit()

cursor.close()
connect.close()
