#!/usr/bin/env python
"""Check database for transactions."""

import mysql.connector

conn = mysql.connector.connect(host='localhost', user='root', password='', database='busmatedb')
cursor = conn.cursor()

cursor.execute('SELECT COUNT(*) FROM transaction')
count = cursor.fetchone()[0]
print('Total transactions:', count)

if count > 0:
    cursor.execute('SELECT MIN(dateID), MAX(dateID) FROM transaction t JOIN date d ON t.dateSID = d.dateSID')
    dates = cursor.fetchone()
    print('Date range:', dates)

    # Check recent transactions
    cursor.execute('SELECT COUNT(*) FROM transaction t JOIN date d ON t.dateSID = d.dateSID WHERE d.dateID >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)')
    recent_count = cursor.fetchone()[0]
    print('Transactions in last 24 months:', recent_count)

cursor.close()
conn.close()