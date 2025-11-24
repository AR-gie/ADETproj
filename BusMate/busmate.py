from flask import Flask, render_template, jsonify, g

try:
    import mysql.connector as mysql_connector
    from mysql.connector import Error
    connector_backend = 'mysql-connector-python'
except ImportError:
    try:
        import pymysql as mysql_connector
        from pymysql import MySQLError as Error
        connector_backend = 'pymysql'
    except ImportError:
        raise ImportError("Neither 'mysql-connector-python' nor 'pymysql' is installed. Install one with: pip install mysql-connector-python OR pip install pymysql")


app = Flask(__name__, static_folder='static', template_folder='templates')

# Database configuration (change password if different)
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '',
    'database': 'busmatedb'
}


def get_db_connection():
    """Return a DB connection stored in Flask's `g` for the request lifecycle."""
    if 'db_conn' not in g:
        try:
            if connector_backend == 'mysql-connector-python':
                g.db_conn = mysql_connector.connect(**DB_CONFIG)
            else:
                # PyMySQL connect signature is similar
                g.db_conn = mysql_connector.connect(host=DB_CONFIG['host'], user=DB_CONFIG['user'], password=DB_CONFIG['password'], database=DB_CONFIG['database'], port=DB_CONFIG['port'])
        except Exception as e:
            app.logger.error('DB connection failed: %s', e)
            raise
    return g.db_conn


@app.teardown_appcontext
def close_db_connection(exception=None):
    conn = g.pop('db_conn', None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


@app.route('/')
def index():
    # Render the static dashboard HTML (templates/Index.html)
    return render_template('Index.html')


@app.route('/q1')
def q1_averages():
    """Return Q1 (Jan-Mar) average totalSum per year as JSON."""
    conn = get_db_connection()
    cursor = conn.cursor()
    query = '''
SELECT d.year,
       ROUND(AVG(t.totalSum), 2) AS avg_q1_total,
       COUNT(*) AS transactions_count
FROM `transaction` t
JOIN `date` d ON t.dateSID = d.dateSID
WHERE MONTH(STR_TO_DATE(d.dateID, '%Y-%m-%d')) BETWEEN 1 AND 3
GROUP BY d.year
ORDER BY d.year;
'''
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    result = []
    for year, avg_q1, cnt in rows:
        result.append({'year': year, 'avg_q1_total': float(avg_q1), 'transactions_count': int(cnt)})
    return jsonify(result)


if __name__ == '__main__':
    print(f'Starting Flask app with DB backend: {connector_backend}')
    app.run(host='0.0.0.0', port=5500, debug=True)
