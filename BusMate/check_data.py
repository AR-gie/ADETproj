import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='busmatedb'
)
cursor = conn.cursor()

# Check available years
cursor.execute('SELECT DISTINCT year FROM date ORDER BY year')
years = cursor.fetchall()
print('Available years in database:')
for year in years:
    print(f'  {year[0]}')

# Check available months for each year
print('\nAvailable months by year:')
cursor.execute('SELECT DISTINCT year, month FROM date ORDER BY year, month')
year_months = cursor.fetchall()
for year, month in year_months:
    print(f'  {year} - {month}')

# Check transaction data range
cursor.execute('SELECT MIN(d.dateID), MAX(d.dateID), COUNT(*) FROM transaction t JOIN date d ON t.dateSID = d.dateSID')
min_date, max_date, count = cursor.fetchone()
print(f'\nTransaction data range:')
print(f'  From: {min_date}')
print(f'  To: {max_date}')
print(f'  Total transactions: {count}')

# Check transactions by year
cursor.execute('SELECT YEAR(STR_TO_DATE(d.dateID, "%Y-%m-%d")), COUNT(*) FROM transaction t JOIN date d ON t.dateSID = d.dateSID GROUP BY YEAR(STR_TO_DATE(d.dateID, "%Y-%m-%d")) ORDER BY YEAR(STR_TO_DATE(d.dateID, "%Y-%m-%d"))')
year_counts = cursor.fetchall()
print(f'\nTransactions by year:')
for year, count in year_counts:
    print(f'  {year}: {count} transactions')

cursor.close()
conn.close()