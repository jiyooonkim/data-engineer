# -*- coding: utf-8 -*-
import os
import jaydebeapi
os.chdir('../../../')


if __name__ == "__main__":
    conn = jaydebeapi.connect(jclassname="org.apache.hive.jdbc.HiveDriver",
                              url='jdbc:hive2://10.65.1.98:10000/default',
                              jars='hive-jdbc-3.1.2-standalone.jar',
                              driver_args={'user': "user", 'password': 'password'}
                              )
    
    cursor = conn.cursor()
    print(cursor)
    # Execute SQL query 
    sql = 'select * from table_'
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)

