from flask import *
import random
from datetime import datetime, timedelta
import threading
import traceback

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
app.secret_key = 'dev-secret'  # change to a secure value in production

# Database configuration (change password if different)
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '',
    'database': 'busmatedb'
}

# In-memory current-lap aggregates (temporary, reset when lap button pressed)
# This avoids persisting lap sums to the database as requested.
lap_lock = threading.Lock()
lap_state = {
    'passengers': 0,
    'total': 0.0,
    'started': datetime.now()
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
    # Render login page
    return render_template('login.html')


@app.route('/login', methods=['POST'])
def login():
    # Retrieve form inputs
    user_id = request.form.get('userID', '').strip()
    user_password = request.form.get('userPassword', '')

    if not user_id:
        flash('Please enter your user ID.')
        return redirect(url_for('index'))

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Try to fetch both userType and userPassword first
        cursor.execute('SELECT userType, userPassword FROM `user` WHERE userID = %s', (user_id,))
        row = cursor.fetchone()
        if row:
            user_type_db = row[0]
            stored_password = row[1] if len(row) > 1 else None
            # If password column exists (not None) verify it
            if stored_password is not None:
                # simple plaintext compare (change to proper hashing in production)
                if str(stored_password) != str(user_password):
                    flash('Invalid credentials.')
                    return redirect(url_for('index'))
            # if stored_password is None, accept login if user exists (fallback)
        else:
            # fallback: maybe table uses userSID or different schema; try matching by userSID numeric
            try:
                # attempt numeric lookup by userSID
                uid_int = int(user_id)
                cursor.execute('SELECT userType FROM `user` WHERE userSID = %s', (uid_int,))
                row2 = cursor.fetchone()
                if not row2:
                    flash('User not found.')
                    return redirect(url_for('index'))
                user_type_db = row2[0]
            except Exception:
                flash('User not found.')
                return redirect(url_for('index'))
    except Exception as e:
        # If the previous SELECT failed because userPassword column doesn't exist,
        # do a simpler lookup by userID -> userType
        try:
            cursor.execute('SELECT userType FROM `user` WHERE userID = %s', (user_id,))
            row = cursor.fetchone()
            if not row:
                flash('User not found (schema mismatch).')
                return redirect(url_for('index'))
            user_type_db = row[0]
        except Exception as e2:
            flash('Database error during login.')
            app.logger.error('Login DB error: %s / %s', e, e2)
            return redirect(url_for('index'))
    finally:
        cursor.close()

    user_type = (user_type_db or '').lower()
    # Store the logged-in user ID in session
    session['user_id'] = user_id
    if user_type == 'manager':
        return redirect(url_for('managepage'))
    else:
        return redirect(url_for('workpage'))


@app.route('/work')
def workpage():
    # Check if user is logged in (session-based)
    if 'user_id' not in session:
        flash('Please log in first.')
        return redirect(url_for('index'))
    
    logged_in_user_id = session['user_id']
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Fetch the logged-in user's full name and userSID
        cursor.execute('SELECT userSID, userID FROM `user` WHERE userID = %s', (logged_in_user_id,))
        user_row = cursor.fetchone()
        if not user_row:
            flash('User not found.')
            return redirect(url_for('index'))
        user_sid = user_row[0]
        user_id = user_row[1]
        
        # Fetch full name (assuming userFN and userLN columns exist)
        cursor.execute('SELECT userFN, userLN FROM `user` WHERE userSID = %s', (user_sid,))
        name_row = cursor.fetchone()
        if name_row:
            personnel = f"{name_row[0]} {name_row[1]}"
        else:
            personnel = user_id
        
        # Fetch all locations (to populate dropdowns and pick random route)
        cursor.execute('SELECT locSID, city, locDistance FROM `loc`')
        all_locs = cursor.fetchall()
        # Fetch customers for dropdown
        cursor.execute('SELECT custSID, custType FROM `customer`')
        all_customers = cursor.fetchall()

        # Persist selected route in session so refreshing the page doesn't change it
        if 'route_from' in session and 'route_to' in session:
            loc_from = session.get('route_from')
            loc_to = session.get('route_to')
            dist_from = session.get('route_dist_from', 0)
            dist_to = session.get('route_dist_to', 0)
            distance = abs(dist_from - dist_to)
            distance_str = f"{distance}km"
        else:
            if len(all_locs) >= 2:
                # Pick two random different locations
                loc_pair = random.sample(all_locs, 2)
                loc_from = loc_pair[0][1]
                loc_to = loc_pair[1][1]
                dist_from = loc_pair[0][2]
                dist_to = loc_pair[1][2]
                # Calculate distance as absolute difference
                distance = abs(dist_from - dist_to)
                distance_str = f"{distance}km"
                # store in session so refresh won't change it
                session['route_from'] = loc_from
                session['route_to'] = loc_to
                session['route_dist_from'] = dist_from
                session['route_dist_to'] = dist_to
            elif len(all_locs) == 1:
                loc_from = all_locs[0][1]
                loc_to = "N/A"
                distance_str = f"{all_locs[0][2]}km"
            else:
                loc_from = "N/A"
                loc_to = "N/A"
                distance_str = "N/A"
        
        # Get current time rounded to nearest hour or 30 minutes
        now = datetime.now()
        minutes = now.minute
        if minutes < 15:
            rounded_time = now.replace(minute=0, second=0, microsecond=0)
        elif minutes < 45:
            rounded_time = now.replace(minute=30, second=0, microsecond=0)
        else:
            rounded_time = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        scheduled_time = rounded_time.strftime('%H:%M')
        # report date shown in dashboard (default today) - prefer session value if set
        report_date = session.get('report_date', datetime.now().date().isoformat())
        
        # Fetch a bus
        cursor.execute('SELECT busID, busLicense FROM `bus` LIMIT 1')
        bus_row = cursor.fetchone()
        bus_number = bus_row[0] if bus_row else 'N/A'
        bus_license = bus_row[1] if bus_row else 'N/A'
    except Exception as e:
        app.logger.error('Error fetching dashboard data: %s', e)
        user_id = 'N/A'
        personnel = 'N/A'
        bus_number = 'N/A'
        bus_license = 'N/A'
        loc_from = 'N/A'
        loc_to = 'N/A'
        distance_str = 'N/A'
        # Provide safe defaults for variables used in the template when an error occurs
        scheduled_time = datetime.now().strftime('%H:%M')
        all_locs = []
        all_customers = []
    finally:
        cursor.close()
    
    return render_template('workpage.html',
                        user_id=user_id,
                        personnel=personnel,
                        bus_number=bus_number,
                        bus_license=bus_license,
                        location_from=loc_from,
                        location_to=loc_to,
                        distance=distance_str,
                        scheduled_time=scheduled_time,
                        report_date=report_date,
                        locs=all_locs,
                        customers=all_customers)


@app.route('/transaction/create', methods=['POST'])
def create_transaction():
    """Create a new transaction from form data."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    try:
        # Get form data (we trust only locSIDs, qty and custSID; other fields computed server-side)
        # locationFrom/locationTo are expected to be locSID integers
        try:
            loc_from_sid = int(request.form.get('locationFrom', 0))
        except Exception:
            loc_from_sid = 0
        try:
            loc_to_sid = int(request.form.get('locationTo', 0))
        except Exception:
            loc_to_sid = 0
        qty = request.form.get('qty', 1, type=int)
        cust_sid = request.form.get('custSID', 1, type=int)
        
        logged_in_user_id = session['user_id']
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get logged-in user's SID
        cursor.execute('SELECT userSID FROM `user` WHERE userID = %s', (logged_in_user_id,))
        user_row = cursor.fetchone()
        if not user_row:
            cursor.close()
            return jsonify({'success': False, 'message': 'User not found'}), 404
        user_sid = user_row[0]
        
        # Get or create a bus (for now, pick first available)
        cursor.execute('SELECT busSID FROM `bus` LIMIT 1')
        bus_row = cursor.fetchone()
        bus_sid = bus_row[0] if bus_row else 1
        
        # Validate or fallback location SIDs
        cursor.execute('SELECT locSID, locDistance FROM `loc` WHERE locSID = %s LIMIT 1', (loc_from_sid,))
        rowf = cursor.fetchone()
        if rowf:
            loc_from_sid = rowf[0]
            dist_from = float(rowf[1])
        else:
            # fallback to any loc
            cursor.execute('SELECT locSID, locDistance FROM `loc` LIMIT 1')
            rowf = cursor.fetchone()
            loc_from_sid = rowf[0] if rowf else 1
            dist_from = float(rowf[1]) if rowf else 0

        cursor.execute('SELECT locSID, locDistance FROM `loc` WHERE locSID = %s LIMIT 1', (loc_to_sid,))
        rowt = cursor.fetchone()
        if rowt:
            loc_to_sid = rowt[0]
            dist_to = float(rowt[1])
        else:
            cursor.execute('SELECT locSID, locDistance FROM `loc` LIMIT 1')
            rowt = cursor.fetchone()
            loc_to_sid = rowt[0] if rowt else 1
            dist_to = float(rowt[1]) if rowt else 0

        # compute distance
        distance_val = abs(dist_from - dist_to)

        # Ensure today's date exists in `date` table; if not, insert it
        today_str = datetime.now().date().isoformat()
        cursor.execute('SELECT dateSID FROM `date` WHERE dateID = %s LIMIT 1', (today_str,))
        date_row = cursor.fetchone()
        if date_row:
            date_sid = date_row[0]
        else:
            # try inserting minimal columns (dateID and year). If your `date` table needs more columns, adjust accordingly.
            year_val = datetime.now().year
            try:
                cursor.execute('INSERT INTO `date` (dateID, year) VALUES (%s, %s)', (today_str, year_val))
                conn.commit()
                date_sid = cursor.lastrowid
            except Exception:
                # fallback: select any dateSID
                cursor.execute('SELECT dateSID FROM `date` LIMIT 1')
                dro = cursor.fetchone()
                date_sid = dro[0] if dro else 1
        
        # Ensure custSID exists or use default
        cursor.execute('SELECT custSID FROM `customer` WHERE custSID = %s', (cust_sid,))
        if not cursor.fetchone():
            cust_sid = 1  # fallback to default customer

        # Compute price and discount according to policy: 20 per km, min 10; discounts by customer
        price_val = max(int(distance_val * 20), 10)
        # qty already read
        if cust_sid == 1:
            discount_val = 0
        elif cust_sid in (2, 3, 4):
            discount_val = int((price_val * qty) * 0.30)
        elif cust_sid == 5:
            discount_val = int((price_val * qty) * 0.20)
        else:
            discount_val = 0

        total_val = price_val * qty - discount_val

        # allow client to pass a refNo (from preview); otherwise generate unique refNo server-side
        ref_no = request.form.get('refNo') or None
        def make_ref():
            return f"REF{datetime.now().strftime('%Y%m%d')}{random.randint(100000,999999)}"
        if not ref_no:
            ref_no = make_ref()
            cursor.execute('SELECT 1 FROM `transaction` WHERE refNo = %s LIMIT 1', (ref_no,))
            attempt = 0
            while cursor.fetchone() and attempt < 5:
                ref_no = make_ref()
                cursor.execute('SELECT 1 FROM `transaction` WHERE refNo = %s LIMIT 1', (ref_no,))
                attempt += 1

        # Allow client override of date/time for testing
        client_date = request.form.get('dateField')
        client_time = request.form.get('time')
        if client_date:
            # use provided dateField as dateID
            date_id_to_use = client_date
            # ensure this date exists in date table (attempt insert if missing)
            cursor.execute('SELECT dateSID FROM `date` WHERE dateID = %s LIMIT 1', (date_id_to_use,))
            dr = cursor.fetchone()
            if dr:
                date_sid = dr[0]
            else:
                year_val = int(client_date.split('-')[0]) if '-' in client_date else datetime.now().year
                try:
                    cursor.execute('INSERT INTO `date` (dateID, year) VALUES (%s, %s)', (date_id_to_use, year_val))
                    conn.commit()
                    date_sid = cursor.lastrowid
                except Exception:
                    cursor.execute('SELECT dateSID FROM `date` LIMIT 1')
                    dro = cursor.fetchone()
                    date_sid = dro[0] if dro else 1
        else:
            client_date = None

        if client_time:
            time_str = client_time
        else:
            time_str = datetime.now().strftime('%H:%M:%S')

        # Insert transaction
        insert_query = '''INSERT INTO `transaction` 
            (userSID, busSID, locSIDfrom, locSIDto, dateSID, custSID, refNo, price, qty, discount, totalSum, time, distance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        # Log the exact INSERT and parameters for debugging
        insert_params = (
            user_sid, bus_sid, loc_from_sid, loc_to_sid, date_sid, cust_sid,
            ref_no, price_val, qty, discount_val, total_val, time_str, int(distance_val)
        )
        app.logger.debug('Executing INSERT into transaction: %s -- params=%s', insert_query, insert_params)
        cursor.execute(insert_query, insert_params)
        
        conn.commit()
        last_id = cursor.lastrowid

        # Diagnostic: confirm the row exists after commit by selecting it back
        try:
            cursor.execute('SELECT transID, refNo FROM `transaction` WHERE refNo = %s LIMIT 1', (ref_no,))
            verify_row = cursor.fetchone()
        except Exception:
            verify_row = None

        # build a transaction datetime string for diagnostics and lap comparison
        used_date_id = date_id_to_use if client_date else today_str
        trans_datetime_str = f"{used_date_id} {time_str}"
        app.logger.debug('Inserted transaction datetime: %s (dateSID=%s)', trans_datetime_str, date_sid)

        # Update in-memory lap aggregates (temporary; not persisted)
        try:
            with lap_lock:
                lap_state['passengers'] = lap_state.get('passengers', 0) + int(qty)
                lap_state['total'] = float(lap_state.get('total', 0.0)) + float(total_val)
        except Exception:
            app.logger.exception('Failed to update in-memory lap aggregates')

        cursor.close()

        resp = {'success': True, 'message': 'Transaction created successfully', 'transID': last_id, 'refNo': ref_no, 'transaction_datetime': trans_datetime_str, 'dateSID': date_sid}
        if verify_row:
            resp['verified'] = True
            resp['verified_transID'] = verify_row[0]
        else:
            resp['verified'] = False

        return jsonify(resp), 201
    except Exception as e:
        # Log full traceback and return it in response for debugging (development only)
        tb = traceback.format_exc()
        app.logger.error('Error creating transaction: %s\n%s', e, tb)
        return jsonify({'success': False, 'message': str(e), 'trace': tb}), 500


@app.route('/transaction/preview', methods=['POST'])
def preview_transaction():
    """Return computed fields for a potential transaction without inserting it."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        # read inputs
        try:
            loc_from_sid = int(request.form.get('locationFrom', 0))
        except Exception:
            loc_from_sid = 0
        try:
            loc_to_sid = int(request.form.get('locationTo', 0))
        except Exception:
            loc_to_sid = 0
        qty = request.form.get('qty', 1, type=int)
        cust_sid = request.form.get('custSID', 1, type=int)

        conn = get_db_connection()
        cursor = conn.cursor()

        # fetch distances
        cursor.execute('SELECT locSID, locDistance FROM `loc` WHERE locSID = %s LIMIT 1', (loc_from_sid,))
        rowf = cursor.fetchone()
        if rowf:
            dist_from = float(rowf[1])
        else:
            cursor.execute('SELECT locSID, locDistance FROM `loc` LIMIT 1')
            rowf = cursor.fetchone()
            dist_from = float(rowf[1]) if rowf else 0

        cursor.execute('SELECT locSID, locDistance FROM `loc` WHERE locSID = %s LIMIT 1', (loc_to_sid,))
        rowt = cursor.fetchone()
        if rowt:
            dist_to = float(rowt[1])
        else:
            cursor.execute('SELECT locSID, locDistance FROM `loc` LIMIT 1')
            rowt = cursor.fetchone()
            dist_to = float(rowt[1]) if rowt else 0

        distance_val = abs(dist_from - dist_to)
        price_val = max(int(distance_val * 20), 10)
        if cust_sid == 1:
            discount_val = 0
        elif cust_sid in (2, 3, 4):
            discount_val = int((price_val * qty) * 0.30)
        elif cust_sid == 5:
            discount_val = int((price_val * qty) * 0.20)
        else:
            discount_val = 0

        total_val = price_val * qty - discount_val

        # generate a preview refNo (non-committed)
        def make_ref():
            return f"REF{datetime.now().strftime('%Y%m%d')}{random.randint(100000,999999)}"
        ref_no = make_ref()
        cursor.execute('SELECT 1 FROM `transaction` WHERE refNo = %s LIMIT 1', (ref_no,))
        attempt = 0
        while cursor.fetchone() and attempt < 5:
            ref_no = make_ref()
            cursor.execute('SELECT 1 FROM `transaction` WHERE refNo = %s LIMIT 1', (ref_no,))
            attempt += 1

        # respect optional client-provided date/time for preview
        client_date = request.form.get('dateField') or datetime.now().date().isoformat()
        client_time = request.form.get('time') or datetime.now().strftime('%H:%M:%S')
        cursor.close()

        return jsonify({
            'success': True,
            'refNo': ref_no,
            'distance': distance_val,
            'price': price_val,
            'qty': qty,
            'discount': discount_val,
            'total': total_val,
            'date': client_date,
            'time': client_time,
            'user_id': session.get('user_id')
        })
    except Exception as e:
        app.logger.error('Preview error: %s', e)
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/manage')
def managepage():
    return render_template('managepage.html')


@app.route('/api/reports')
def api_reports():
    """Return JSON with current lap and overall transaction summaries."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Use in-memory lap_state for current-lap aggregates (temporary, not persisted)
    with lap_lock:
        lap_number = None
        lap_started = lap_state.get('started', datetime.now())
        current_count = int(lap_state.get('passengers', 0))
        current_sum = float(lap_state.get('total', 0.0))

    # overall or date-filtered totals (for report summary)
    date_filter = request.args.get('date')
    try:
        if date_filter:
            # totals for the requested report date
            cursor.execute('''SELECT COUNT(*), COALESCE(SUM(totalSum),0) FROM `transaction` t
                            JOIN `date` d ON t.dateSID = d.dateSID
                            WHERE d.dateID = %s''', (date_filter,))
            all_row = cursor.fetchone()
            total_count = int(all_row[0]) if all_row and all_row[0] is not None else 0
            total_sum = float(all_row[1]) if all_row and all_row[1] is not None else 0.0
        else:
            cursor.execute('SELECT COUNT(*), COALESCE(SUM(totalSum),0) FROM `transaction`')
            all_row = cursor.fetchone()
            total_count = int(all_row[0]) if all_row and all_row[0] is not None else 0
            total_sum = float(all_row[1]) if all_row and all_row[1] is not None else 0.0
    except Exception:
        total_count = 0
        total_sum = 0.0

    cursor.close()

    return jsonify({
        'lap_number': lap_number,
        'lap_started': lap_started.isoformat() if isinstance(lap_started, datetime) else str(lap_started),
        'current_passengers': current_count,
        'current_total': current_sum,
        'total_passengers': total_count,
        'total_fare': total_sum
    })


@app.route('/api/lap/reset', methods=['POST'])
def api_lap_reset():
    """Start a new lap: increment lap_number and set lap_started to now."""
    # Reset in-memory lap aggregates (temporary; not persisted)
    try:
        now = datetime.now()
        with lap_lock:
            lap_state['passengers'] = 0
            lap_state['total'] = 0.0
            lap_state['started'] = now
        return jsonify({'success': True, 'message': 'Lap reset', 'lap_started': now.isoformat()}), 200
    except Exception as e:
        tb = traceback.format_exc()
        app.logger.error('Lap reset error: %s\n%s', e, tb)
        return jsonify({'success': False, 'message': str(e), 'trace': tb}), 500


@app.route('/api/report-date', methods=['POST'])
def api_set_report_date():
    """Set the user's report date preference in session so it persists across refreshes."""
    if not request.form and request.is_json:
        payload = request.get_json()
        new_date = payload.get('date')
    else:
        new_date = request.form.get('date')

    if not new_date:
        return jsonify({'success': False, 'message': 'date is required'}), 400
    try:
        # basic validation: yyyy-mm-dd
        # store in session
        session['report_date'] = new_date
        return jsonify({'success': True, 'report_date': new_date}), 200
    except Exception as e:
        tb = traceback.format_exc()
        app.logger.error('Set report date error: %s\n%s', e, tb)
        return jsonify({'success': False, 'message': str(e), 'trace': tb}), 500


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
