from flask import *
import random
from datetime import datetime, timedelta
import threading
import traceback
from flask import render_template, request, send_file
# import pandas as pd
# import sys
# import subprocess

# def ensure_package(package_name, import_name=None):
#     module = import_name if import_name else package_name

#     try:
#         __import__(module)
#         print(f"[OK] {package_name} already installed")
#     except ImportError:
#         print(f"[INSTALL] Installing {package_name}...")
#         subprocess.check_call([
#             sys.executable, "-m", "pip", "install", package_name
#         ])
#         # Verify installation
#         __import__(module)
#         print(f"[DONE] {package_name} installed successfully")

# ensure_package("flask")
# ensure_package("pandas")
# ensure_package("python-docx", "docx")

# from docx import Document

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


def ensure_user_bus_assignment_table():
    """Create user_bus_assignment table if it doesn't exist."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS `user_bus_assignment` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `userSID` VARCHAR(50) NOT NULL,
                `busSID` VARCHAR(50) NOT NULL,
                UNIQUE KEY `unique_assignment` (`userSID`, `busSID`),
                FOREIGN KEY (`userSID`) REFERENCES `user`(`userSID`) ON DELETE CASCADE,
                FOREIGN KEY (`busSID`) REFERENCES `bus`(`busSID`) ON DELETE CASCADE
            )
        ''')
        conn.commit()
        cursor.close()
    except Exception as e:
        app.logger.error('Error creating user_bus_assignment table: %s', e)


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
        return redirect(url_for('cool_animation', redirect=url_for('managepage')))
    else:
        return redirect(url_for('cool_animation', redirect=url_for('workpage')))


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('cool_animation', redirect=url_for('index')))


@app.route('/cool')
def cool_animation():
    return render_template('cool_animation.html')


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
    if 'user_id' not in session:
        flash('Please log in first.')
        return redirect(url_for('index'))
    
    logged_in_user_id = session['user_id']
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Fetch the logged-in user's ID and name for display
        cursor.execute('SELECT userID, userFN, userLN FROM `user` WHERE userID = %s', (logged_in_user_id,))
        user_row = cursor.fetchone()
        user_id = user_row[0] if user_row else logged_in_user_id
        user_name = f"{user_row[1]} {user_row[2]}" if user_row else "Unknown User"
    except Exception as e:
        app.logger.error('Error fetching manager data: %s', e)
        user_id = logged_in_user_id
        user_name = "Unknown User"
    finally:
        cursor.close()
    
    return render_template('managepage.html', user_id=user_id, user_name=user_name, current_date=datetime.now().strftime('%Y-%m-%d'), from_date=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))


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


@app.route('/api/manager/filter-options')
def api_manager_filter_options():
    """Return available buses, routes, and customer types for dashboard filters."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Fetch all buses
        cursor.execute('SELECT busSID, busID FROM `bus` ORDER BY busID')
        buses = [{'id': row[0], 'number': row[1]} for row in cursor.fetchall()]
        
        # Fetch all routes (location pairs from transactions)
        cursor.execute('''SELECT DISTINCT lf.locSID, lf.city, lt.city 
                         FROM `loc` lf 
                         JOIN `loc` lt ON 1=1 
                         WHERE lf.locSID != lt.locSID
                         ORDER BY lf.city, lt.city 
                         LIMIT 20''')
        routes = [{'id': f"{row[0]}", 'from_location': row[1], 'to_location': row[2]} for row in cursor.fetchall()]
        
        # Fetch all customer types
        cursor.execute('SELECT custSID, custType FROM `customer` ORDER BY custType')
        customer_types = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]
        
        # Fetch all users
        cursor.execute('SELECT userSID, userID FROM `user` ORDER BY userID')
        users = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]
        
        cursor.close()
        return jsonify({
            'buses': buses,
            'routes': routes,
            'customerTypes': customer_types,
            'users': users
        })
    except Exception as e:
        app.logger.error('Error loading filter options: %s', e)
        cursor.close()
        return jsonify({'buses': [], 'routes': [], 'customerTypes': []}), 500


@app.route('/api/manager/timeline-data')
def api_manager_timeline_data():
    """Return time-series data for revenue and customer count with different grouping options."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    group_by = request.args.get('group_by', 'monthly')
    
    # Build WHERE clause based on filters
    where_parts = []
    params = []
    
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    bus_id = request.args.get('bus_id')
    customer_type = request.args.get('customer_type')
    user_id = request.args.get('user_id')
    year = request.args.get('year')
    month = request.args.get('month')
    
    if from_date:
        where_parts.append('d.dateID >= %s')
        params.append(from_date)
    if to_date:
        where_parts.append('d.dateID <= %s')
        params.append(to_date)
    if bus_id:
        where_parts.append('t.busSID = %s')
        params.append(int(bus_id))
    if customer_type:
        where_parts.append('c.custType = %s')
        params.append(customer_type)
    if user_id:
        where_parts.append('u.userID = %s')
        params.append(user_id)
    if year:
        where_parts.append('d.year = %s')
        params.append(int(year))
    if month:
        where_parts.append('d.month = %s')
        params.append(month)
    
    where_clause = ' AND '.join(where_parts) if where_parts else '1=1'
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if group_by == 'daily':
            query = f'''SELECT d.dateID, COALESCE(SUM(t.totalSum), 0), COUNT(*)
                       FROM `date` d
                       LEFT JOIN `transaction` t ON d.dateSID = t.dateSID
                       LEFT JOIN `customer` c ON t.custSID = c.custSID
                       LEFT JOIN `user` u ON t.userSID = u.userSID
                       WHERE {where_clause}
                       GROUP BY d.dateSID, d.dateID
                       ORDER BY d.dateID'''
            cursor.execute(query, params)
            
        elif group_by == 'weekly':
            weekly_year = request.args.get('weekly_year', '')
            weekly_month = request.args.get('weekly_month', '')
            
            where_parts = []
            params = []
            
            if from_date:
                where_parts.append('d.dateID >= %s')
                params.append(from_date)
            if to_date:
                where_parts.append('d.dateID <= %s')
                params.append(to_date)
            if bus_id:
                where_parts.append('t.busSID = %s')
                params.append(int(bus_id))
            if customer_type:
                where_parts.append('c.custType = %s')
                params.append(customer_type)
            if user_id:
                where_parts.append('u.userID = %s')
                params.append(user_id)
            if year:
                where_parts.append('d.year = %s')
                params.append(int(year))
            if month:
                where_parts.append('d.month = %s')
                params.append(month)
            if weekly_year:
                where_parts.append('YEAR(STR_TO_DATE(d.dateID, \'%Y-%m-%d\')) = %s')
                params.append(int(weekly_year))
            if weekly_month:
                where_parts.append('MONTH(STR_TO_DATE(d.dateID, \'%Y-%m-%d\')) = %s')
                params.append(int(weekly_month))
            
            where_clause = ' AND '.join(where_parts) if where_parts else '1=1'
            
            query = f'''SELECT 
                          CONCAT(YEAR(STR_TO_DATE(d.dateID, '%Y-%m-%d')), '-W', 
                          LPAD(WEEK(STR_TO_DATE(d.dateID, '%Y-%m-%d')), 2, '0')) as week_label,
                          COALESCE(SUM(t.totalSum), 0), 
                          COUNT(*)
                      FROM `date` d
                      LEFT JOIN `transaction` t ON d.dateSID = t.dateSID
                      LEFT JOIN `customer` c ON t.custSID = c.custSID
                      LEFT JOIN `user` u ON t.userSID = u.userSID
                      WHERE {where_clause}
                      GROUP BY YEAR(STR_TO_DATE(d.dateID, '%Y-%m-%d')), 
                               WEEK(STR_TO_DATE(d.dateID, '%Y-%m-%d'))
                      ORDER BY week_label'''
            cursor.execute(query, params)
            
        elif group_by == 'quarterly':
            quarterly_start = request.args.get('quarterly_start', '')
            quarterly_end = request.args.get('quarterly_end', '')
            
            if not quarterly_start:
                quarterly_start = str(datetime.now().year - 2)
            if not quarterly_end:
                quarterly_end = str(datetime.now().year)
            
            # Build additional WHERE conditions for year and month
            extra_conditions = []
            extra_params = []
            if year:
                extra_conditions.append('YEAR(STR_TO_DATE(d.dateID, \'%Y-%m-%d\')) = %s')
                extra_params.append(int(year))
            if month:
                # For quarterly view, filter by quarter containing the month
                month_num = datetime.strptime(month, '%B').month
                quarter = ((month_num - 1) // 3) + 1
                extra_conditions.append('QUARTER(STR_TO_DATE(d.dateID, \'%Y-%m-%d\')) = %s')
                extra_params.append(quarter)
            
            extra_where = ' AND ' + ' AND '.join(extra_conditions) if extra_conditions else ''
            
            query = f'''SELECT 
                          CONCAT(YEAR(STR_TO_DATE(d.dateID, '%Y-%m-%d')), '-Q',
                          QUARTER(STR_TO_DATE(d.dateID, '%Y-%m-%d'))) as quarter_label,
                          COALESCE(SUM(t.totalSum), 0), 
                          COUNT(*)
                      FROM `date` d
                      LEFT JOIN `transaction` t ON d.dateSID = t.dateSID
                      LEFT JOIN `customer` c ON t.custSID = c.custSID
                      LEFT JOIN `user` u ON t.userSID = u.userSID
                      WHERE YEAR(STR_TO_DATE(d.dateID, '%Y-%m-%d')) BETWEEN %s AND %s
                      AND (%s IS NULL OR d.dateID >= %s)
                      AND (%s IS NULL OR d.dateID <= %s)
                      AND (%s IS NULL OR t.busSID = %s)
                      AND (%s IS NULL OR c.custType = %s)
                      AND (%s IS NULL OR u.userID = %s){extra_where}
                      GROUP BY YEAR(STR_TO_DATE(d.dateID, '%Y-%m-%d')), 
                               QUARTER(STR_TO_DATE(d.dateID, '%Y-%m-%d'))
                      ORDER BY quarter_label'''
            cursor.execute(query, (int(quarterly_start), int(quarterly_end), from_date, from_date, to_date, to_date, bus_id, bus_id, customer_type, customer_type, user_id, user_id) + tuple(extra_params))
            
        elif group_by == 'yearly':
            yearly_start = request.args.get('yearly_start', '')
            yearly_end = request.args.get('yearly_end', '')
            
            if not yearly_start:
                yearly_start = str(datetime.now().year - 2)
            if not yearly_end:
                yearly_end = str(datetime.now().year)
            
            # Build additional WHERE conditions for year and month
            extra_conditions = []
            extra_params = []
            if year:
                # For yearly view, override the BETWEEN condition
                yearly_start = year
                yearly_end = year
            if month:
                # For yearly view with month filter, we need to filter by month within the year range
                extra_conditions.append('MONTH(STR_TO_DATE(d.dateID, \'%Y-%m-%d\')) = %s')
                extra_params.append(datetime.strptime(month, '%B').month)
            
            extra_where = ' AND ' + ' AND '.join(extra_conditions) if extra_conditions else ''
            
            query = f'''SELECT 
                          YEAR(STR_TO_DATE(d.dateID, '%Y-%m-%d')) as year_label,
                          COALESCE(SUM(t.totalSum), 0), 
                          COUNT(*)
                      FROM `date` d
                      LEFT JOIN `transaction` t ON d.dateSID = t.dateSID
                      LEFT JOIN `customer` c ON t.custSID = c.custSID
                      LEFT JOIN `user` u ON t.userSID = u.userSID
                      WHERE YEAR(STR_TO_DATE(d.dateID, '%Y-%m-%d')) BETWEEN %s AND %s
                      AND (%s IS NULL OR d.dateID >= %s)
                      AND (%s IS NULL OR d.dateID <= %s)
                      AND (%s IS NULL OR t.busSID = %s)
                      AND (%s IS NULL OR c.custType = %s)
                      AND (%s IS NULL OR u.userID = %s){extra_where}
                      GROUP BY YEAR(STR_TO_DATE(d.dateID, '%Y-%m-%d'))
                      ORDER BY year_label'''
            cursor.execute(query, (int(yearly_start), int(yearly_end), from_date, from_date, to_date, to_date, bus_id, bus_id, customer_type, customer_type, user_id, user_id) + tuple(extra_params))
        
        else:  # default to monthly
            monthly_year = request.args.get('monthly_year', '')
            
            # Build WHERE conditions for year and month filters
            where_parts = ['d.dateID >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)']
            params = []
            
            if from_date:
                where_parts.append('d.dateID >= %s')
                params.append(from_date)
            if to_date:
                where_parts.append('d.dateID <= %s')
                params.append(to_date)
            if bus_id:
                where_parts.append('t.busSID = %s')
                params.append(int(bus_id))
            if customer_type:
                where_parts.append('c.custType = %s')
                params.append(customer_type)
            if user_id:
                where_parts.append('u.userID = %s')
                params.append(user_id)
            if year:
                where_parts.append('YEAR(STR_TO_DATE(d.dateID, \'%Y-%m-%d\')) = %s')
                params.append(int(year))
            if month:
                where_parts.append('MONTH(STR_TO_DATE(d.dateID, \'%Y-%m-%d\')) = %s')
                params.append(datetime.strptime(month, '%B').month)
            
            where_clause = ' AND '.join(where_parts)
            
            if monthly_year:
                where_clause += ' AND YEAR(STR_TO_DATE(d.dateID, \'%Y-%m-%d\')) = %s'
                params.append(int(monthly_year))
            
            query = f'''SELECT 
                          DATE_FORMAT(STR_TO_DATE(d.dateID, '%Y-%m-%d'), '%Y-%m') as month_label,
                          COALESCE(SUM(t.totalSum), 0), 
                          COUNT(*)
                      FROM `date` d
                      LEFT JOIN `transaction` t ON d.dateSID = t.dateSID
                      LEFT JOIN `customer` c ON t.custSID = c.custSID
                      LEFT JOIN `user` u ON t.userSID = u.userSID
                      WHERE {where_clause}
                      GROUP BY DATE_FORMAT(STR_TO_DATE(d.dateID, '%Y-%m-%d'), '%Y-%m')
                      ORDER BY month_label'''
            cursor.execute(query, params)
        
        rows = cursor.fetchall()
        cursor.close()
        
        labels = []
        revenue = []
        customers = []
        
        for row in rows:
            if row[0]:
                labels.append(str(row[0]))
                revenue.append(float(row[1]) if row[1] is not None else 0)
                customers.append(int(row[2]) if row[2] is not None else 0)
        
        return jsonify({
            'labels': labels,
            'revenue': revenue,
            'customers': customers
        })
    except Exception as e:
        app.logger.error('Error loading timeline data: %s', e)
        cursor.close()
        return jsonify({'labels': [], 'revenue': [], 'customers': []}), 500


@app.route('/api/manager/dashboard-data')
def api_manager_dashboard_data():
    """Return dashboard data with optional filters: customer count, revenue, customer type distribution, and top routes."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Build WHERE clause based on filters
        where_parts = []
        params = []
        
        from_date = request.args.get('from_date')
        to_date = request.args.get('to_date')
        bus_id = request.args.get('bus_id')
        route_id = request.args.get('route_id')
        customer_type = request.args.get('customer_type')
        user_id = request.args.get('user_id')
        
        if from_date:
            where_parts.append('d.dateID >= %s')
            params.append(from_date)
        if to_date:
            where_parts.append('d.dateID <= %s')
            params.append(to_date)
        if bus_id:
            where_parts.append('t.busSID = %s')
            params.append(int(bus_id))
        if route_id:
            where_parts.append('t.locSIDfrom = %s')
            params.append(int(route_id))
        if customer_type:
            where_parts.append('c.custType = %s')
            params.append(customer_type)
        if user_id:
            where_parts.append('u.userID = %s')
            params.append(user_id)
        
        where_clause = ' AND '.join(where_parts) if where_parts else '1=1'
        
        # Total transactions (count of all transactions)
        query_transactions = f'''SELECT COUNT(*) FROM `transaction` t
                                 JOIN `date` d ON t.dateSID = d.dateSID
                                 JOIN `customer` c ON t.custSID = c.custSID
                                 JOIN `user` u ON t.userSID = u.userSID
                                 WHERE {where_clause}'''
        cursor.execute(query_transactions, params)
        total_transactions = cursor.fetchone()[0] or 0
        
        # Total customers (count of all transactions as proxy for customer activity)
        total_customers = total_transactions
        
        # Total revenue (sum of totalSum)
        query_revenue = f'''SELECT COALESCE(SUM(t.totalSum), 0) FROM `transaction` t
                            JOIN `date` d ON t.dateSID = d.dateSID
                            JOIN `customer` c ON t.custSID = c.custSID
                            JOIN `user` u ON t.userSID = u.userSID
                            WHERE {where_clause}'''
        cursor.execute(query_revenue, params)
        total_revenue = float(cursor.fetchone()[0]) or 0.0
        
        # Customer types distribution (pie chart data)
        query_customer_types = f'''SELECT c.custType, COUNT(*) as count
                                   FROM `transaction` t
                                   JOIN `customer` c ON t.custSID = c.custSID
                                   JOIN `date` d ON t.dateSID = d.dateSID
                                   JOIN `user` u ON t.userSID = u.userSID
                                   WHERE {where_clause}
                                   GROUP BY c.custType
                                   ORDER BY count DESC'''
        cursor.execute(query_customer_types, params)
        customer_types = [{'name': row[0], 'count': row[1]} for row in cursor.fetchall()]
        
        # Top routes with From/To/Combined counts
        query_routes = f'''SELECT lf.city,
                                  SUM(CASE WHEN t.locSIDfrom = lf.locSID THEN 1 ELSE 0 END) as from_count,
                                  SUM(CASE WHEN t.locSIDto = lf.locSID THEN 1 ELSE 0 END) as to_count,
                                  SUM(CASE WHEN t.locSIDfrom = lf.locSID OR t.locSIDto = lf.locSID THEN 1 ELSE 0 END) as combined_count
                           FROM `transaction` t
                           JOIN `loc` lf ON t.locSIDfrom = lf.locSID OR t.locSIDto = lf.locSID
                           JOIN `date` d ON t.dateSID = d.dateSID
                           JOIN `customer` c ON t.custSID = c.custSID
                           JOIN `user` u ON t.userSID = u.userSID
                           WHERE {where_clause}
                           GROUP BY lf.locSID, lf.city
                           ORDER BY combined_count DESC
                           LIMIT 10'''
        cursor.execute(query_routes, params)
        top_routes = [{'location': row[0], 'from_count': row[1], 'to_count': row[2], 'combined_count': row[3]} 
                      for row in cursor.fetchall()]
        
        cursor.close()
        
        return jsonify({
            'totalCustomers': total_customers,
            'totalTransactions': total_transactions,
            'totalRevenue': total_revenue,
            'customerTypes': customer_types,
            'topRoutes': top_routes
        })
    except Exception as e:
        app.logger.error('Error loading dashboard data: %s', e)
        cursor.close()
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/users')
def api_manager_users():
    """Return list of all users."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query users table with all relevant columns
        cursor.execute('SELECT `userSID`, `userID`, `userType`, `userFN`, `userLN`, `userLicense`, `userPhone`, `userEmail` FROM `user` ORDER BY `userID`')
        rows = cursor.fetchall()
        
        app.logger.info('User query returned %d rows', len(rows))
        
        users = []
        for row in rows:
            users.append({
                'userSID': int(row[0]),
                'userID': str(row[1]),
                'userType': str(row[2]),
                'userFN': str(row[3]) if row[3] else '',
                'userLN': str(row[4]) if row[4] else '',
                'userLicense': str(row[5]) if row[5] else '',
                'userPhone': str(row[6]) if row[6] else '',
                'userEmail': str(row[7]) if row[7] else ''
            })
        
        cursor.close()
        app.logger.info('Returning %d users', len(users))
        return jsonify({'users': users})
        
    except Exception as e:
        app.logger.error('Error loading users: %s', e)
        app.logger.error('Traceback: %s', traceback.format_exc())
        try:
            cursor.close()
        except:
            pass
        return jsonify({'users': [], 'error': str(e)}), 500


@app.route('/api/manager/buses')
def api_manager_buses():
    """Return list of all buses."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query buses table directly
        cursor.execute('SELECT `busSID`, `busID`, `busLicense`, `busType` FROM `bus` ORDER BY `busID`')
        rows = cursor.fetchall()
        
        app.logger.info('Bus query returned %d rows', len(rows))
        
        buses = []
        for row in rows:
            buses.append({
                'busSID': int(row[0]),
                'busID': str(row[1]),
                'busLicense': str(row[2]),
                'busType': str(row[3])
            })
        
        cursor.close()
        app.logger.info('Returning %d buses', len(buses))
        return jsonify({'buses': buses})
        
    except Exception as e:
        app.logger.error('Error loading buses: %s', e)
        app.logger.error('Traceback: %s', traceback.format_exc())
        try:
            cursor.close()
        except:
            pass
        return jsonify({'buses': [], 'error': str(e)}), 500


@app.route('/api/manager/analytics-summary')
def api_manager_analytics_summary():
    """Return advanced analytics summary data."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Average revenue per transaction
        cursor.execute('SELECT AVG(totalSum) FROM `transaction`')
        avg_revenue = cursor.fetchone()[0] or 0
        
        # Total transactions
        cursor.execute('SELECT COUNT(*) FROM `transaction`')
        total_transactions = cursor.fetchone()[0] or 0
        
        # Average customers per day
        cursor.execute('''SELECT AVG(daily_count) FROM (
                         SELECT COUNT(DISTINCT custSID) as daily_count FROM `transaction` 
                         GROUP BY DATE(CONCAT(d.dateID))
                         ) as daily_stats''')
        avg_customers_day = cursor.fetchone()[0] or 0
        
        # Top location
        cursor.execute('''SELECT lf.city FROM `transaction` t
                         JOIN `loc` lf ON t.locSIDfrom = lf.locSID
                         GROUP BY lf.locSID
                         ORDER BY COUNT(*) DESC
                         LIMIT 1''')
        top_loc = cursor.fetchone()
        top_location = top_loc[0] if top_loc else 'N/A'
        
        # Top customer type
        cursor.execute('''SELECT c.custType FROM `transaction` t
                         JOIN `customer` c ON t.custSID = c.custSID
                         GROUP BY c.custSID
                         ORDER BY COUNT(*) DESC
                         LIMIT 1''')
        top_cust = cursor.fetchone()
        top_customer_type = top_cust[0] if top_cust else 'N/A'
        
        cursor.close()
        
        return jsonify({
            'avgRevenue': float(avg_revenue),
            'avgCustomersDay': float(avg_customers_day),
            'totalTransactions': int(total_transactions),
            'topLocation': top_location,
            'topCustomerType': top_customer_type,
            'peakHour': '10:00-11:00',
            'growthRate': '12.5'
        })
    except Exception as e:
        app.logger.error('Error loading analytics summary: %s', e)
        cursor.close()
        return jsonify({
            'avgRevenue': 0,
            'avgCustomersDay': 0,
            'totalTransactions': 0,
            'topLocation': 'N/A',
            'topCustomerType': 'N/A',
            'peakHour': 'N/A',
            'growthRate': '0'
        }), 500


@app.route('/api/manager/update-user', methods=['POST'])
def api_manager_update_user():
    """Update user information."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    data = request.get_json()
    userSID = data.get('userSID')
    
    if not userSID:
        return jsonify({'success': False, 'message': 'Missing userSID'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Build dynamic UPDATE statement based on provided fields
        update_fields = []
        params = []
        
        if 'userFN' in data:
            update_fields.append('userFN = %s')
            params.append(data['userFN'])
        if 'userLN' in data:
            update_fields.append('userLN = %s')
            params.append(data['userLN'])
        if 'userLicense' in data:
            update_fields.append('userLicense = %s')
            params.append(data['userLicense'])
        if 'userPhone' in data:
            update_fields.append('userPhone = %s')
            params.append(data['userPhone'])
        if 'userEmail' in data:
            update_fields.append('userEmail = %s')
            params.append(data['userEmail'])
        if 'userType' in data:
            update_fields.append('userType = %s')
            params.append(data['userType'])
        
        if not update_fields:
            return jsonify({'success': False, 'message': 'No fields to update'}), 400
        
        params.append(userSID)
        update_query = 'UPDATE `user` SET ' + ', '.join(update_fields) + ' WHERE userSID = %s'
        
        cursor.execute(update_query, params)
        conn.commit()
        cursor.close()
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error('Error updating user: %s', e)
        cursor.close()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/manager/delete-user', methods=['POST'])
def api_manager_delete_user():
    """Delete a user."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    data = request.get_json()
    userSID = data.get('userSID')
    
    if not userSID:
        return jsonify({'success': False, 'message': 'Missing userSID'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Delete from user_bus_assignment first (foreign key)
        cursor.execute('DELETE FROM `user_bus_assignment` WHERE userSID = %s', (userSID,))
        # Then delete the user
        cursor.execute('DELETE FROM `user` WHERE userSID = %s', (userSID,))
        conn.commit()
        cursor.close()
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error('Error deleting user: %s', e)
        cursor.close()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/manager/user-bus-assignments')
def api_manager_user_bus_assignments():
    """Get buses assigned to a specific user."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    userSID = request.args.get('userSID')
    
    if not userSID:
        return jsonify({'assignments': []}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT busSID FROM `user_bus_assignment` WHERE userSID = %s', (userSID,))
        assignments = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return jsonify({'assignments': assignments})
    except Exception as e:
        app.logger.error('Error loading assignments: %s', e)
        cursor.close()
        return jsonify({'assignments': []}), 500


@app.route('/api/manager/bus-assigned-users')
def api_manager_bus_assigned_users():
    """Get users assigned to a specific bus."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    busSID = request.args.get('busSID')
    
    if not busSID:
        return jsonify({'users': []}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT u.userID, u.userType 
            FROM `user` u
            JOIN `user_bus_assignment` uba ON u.userSID = uba.userSID
            WHERE uba.busSID = %s
            ORDER BY u.userID
        ''', (busSID,))
        users = [{'userID': row[0], 'userType': row[1]} for row in cursor.fetchall()]
        cursor.close()
        return jsonify({'users': users})
    except Exception as e:
        app.logger.error('Error loading bus users: %s', e)
        cursor.close()
        return jsonify({'users': []}), 500


@app.route('/api/manager/delete-bus', methods=['POST'])
def api_manager_delete_bus():
    """Delete a bus."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    data = request.get_json()
    busSID = data.get('busSID')
    
    if not busSID:
        return jsonify({'success': False, 'message': 'Missing busSID'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Delete from user_bus_assignment first (foreign key)
        cursor.execute('DELETE FROM `user_bus_assignment` WHERE busSID = %s', (busSID,))
        # Then delete the bus
        cursor.execute('DELETE FROM `bus` WHERE busSID = %s', (busSID,))
        conn.commit()
        cursor.close()
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error('Error deleting bus: %s', e)
        cursor.close()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/manager/assign-user-to-buses', methods=['POST'])
def api_manager_assign_user_to_buses():
    """Assign user to multiple buses."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    data = request.get_json()
    userSID = data.get('userSID')
    busSIDs = data.get('busSIDs', [])
    
    if not userSID:
        return jsonify({'success': False, 'message': 'Missing userSID'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Delete existing assignments
        cursor.execute('DELETE FROM `user_bus_assignment` WHERE userSID = %s', (userSID,))
        
        # Add new assignments
        for busSID in busSIDs:
            cursor.execute('INSERT INTO `user_bus_assignment` (userSID, busSID) VALUES (%s, %s)', 
                          (userSID, busSID))
        
        conn.commit()
        cursor.close()
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error('Error assigning user to buses: %s', e)
        cursor.close()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/manager/export')
def api_manager_export():
    """Export dashboard data as CSV."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build WHERE clause based on filters
        where_parts = []
        params = []
        
        from_date = request.args.get('from_date')
        to_date = request.args.get('to_date')
        bus_id = request.args.get('bus_id')
        route_id = request.args.get('route_id')
        customer_type = request.args.get('customer_type')
        user_id = request.args.get('user_id')
        
        if from_date:
            where_parts.append('d.dateID >= %s')
            params.append(from_date)
        if to_date:
            where_parts.append('d.dateID <= %s')
            params.append(to_date)
        if bus_id:
            where_parts.append('t.busSID = %s')
            params.append(int(bus_id))
        if route_id:
            where_parts.append('t.locSIDfrom = %s')
            params.append(int(route_id))
        if customer_type:
            where_parts.append('c.custType = %s')
            params.append(customer_type)
        if user_id:
            where_parts.append('u.userID = %s')
            params.append(user_id)
        
        where_clause = ' AND '.join(where_parts) if where_parts else '1=1'
        
        # Fetch transaction data
        query = f'''SELECT t.refNo, d.dateID, t.time, b.busID, lf.city, lt.city, 
                           c.custType, t.distance, t.price, t.qty, t.discount, t.totalSum,
                           u.userID, u.userType
                    FROM `transaction` t
                    JOIN `date` d ON t.dateSID = d.dateSID
                    JOIN `bus` b ON t.busSID = b.busSID
                    JOIN `loc` lf ON t.locSIDfrom = lf.locSID
                    JOIN `loc` lt ON t.locSIDto = lt.locSID
                    JOIN `customer` c ON t.custSID = c.custSID
                    JOIN `user` u ON t.userSID = u.userSID
                    WHERE {where_clause}
                    ORDER BY d.dateID, t.time'''
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        
        # Create CSV response
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Reference No', 'Date', 'Time', 'Bus', 'From', 'To', 'Customer Type', 'Distance (km)', 'Price', 'Quantity', 'Discount', 'Total'])
        
        for row in rows:
            writer.writerow(row)
        
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=busmate_export.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response
    except Exception as e:
        app.logger.error('Error exporting data: %s', e)
        return jsonify({'error': str(e)}), 500


@app.route('/api/manager/add-user', methods=['POST'])
def api_manager_add_user():
    """Add a new user."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    data = request.get_json()
    userID = data.get('userID')
    userType = data.get('userType', 'bus_worker')
    userFN = data.get('userFN', '')
    userLN = data.get('userLN', '')
    userLicense = data.get('userLicense', '')
    userPhone = data.get('userPhone', '')
    userEmail = data.get('userEmail', '')
    
    if not userID:
        return jsonify({'success': False, 'message': 'userID is required'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if userID already exists
        cursor.execute('SELECT userSID FROM `user` WHERE userID = %s', (userID,))
        if cursor.fetchone():
            return jsonify({'success': False, 'message': 'User ID already exists'}), 400
        
        # Insert new user
        cursor.execute('''
            INSERT INTO `user` (userID, userType, userFN, userLN, userLicense, userPhone, userEmail)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (userID, userType, userFN, userLN, userLicense, userPhone, userEmail))
        conn.commit()
        new_user_sid = cursor.lastrowid
        cursor.close()
        return jsonify({'success': True, 'userSID': new_user_sid})
    except Exception as e:
        app.logger.error('Error adding user: %s', e)
        cursor.close()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/manager/add-bus', methods=['POST'])
def api_manager_add_bus():
    """Add a new bus."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    data = request.get_json()
    busID = data.get('busID')
    busLicense = data.get('busLicense')
    busType = data.get('busType', 'coach')
    
    if not busID or not busLicense:
        return jsonify({'success': False, 'message': 'busID and busLicense are required'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if busID already exists
        cursor.execute('SELECT busSID FROM `bus` WHERE busID = %s', (busID,))
        if cursor.fetchone():
            return jsonify({'success': False, 'message': 'Bus ID already exists'}), 400
        
        # Insert new bus
        cursor.execute('''
            INSERT INTO `bus` (busID, busLicense, busType)
            VALUES (%s, %s, %s)
        ''', (busID, busLicense, busType))
        conn.commit()
        new_bus_sid = cursor.lastrowid
        cursor.close()
        return jsonify({'success': True, 'busSID': new_bus_sid})
    except Exception as e:
        app.logger.error('Error adding bus: %s', e)
        cursor.close()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/manager/update-bus', methods=['POST'])
def api_manager_update_bus():
    """Update bus information."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    data = request.get_json()
    busSID = data.get('busSID')
    
    if not busSID:
        return jsonify({'success': False, 'message': 'Missing busSID'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Build dynamic UPDATE statement
        update_fields = []
        params = []
        
        if 'busID' in data:
            update_fields.append('busID = %s')
            params.append(data['busID'])
        if 'busLicense' in data:
            update_fields.append('busLicense = %s')
            params.append(data['busLicense'])
        if 'busType' in data:
            update_fields.append('busType = %s')
            params.append(data['busType'])
        
        if not update_fields:
            return jsonify({'success': False, 'message': 'No fields to update'}), 400
        
        params.append(busSID)
        update_query = 'UPDATE `bus` SET ' + ', '.join(update_fields) + ' WHERE busSID = %s'
        
        cursor.execute(update_query, params)
        conn.commit()
        cursor.close()
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error('Error updating bus: %s', e)
        cursor.close()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/manager/bus-details')
def api_manager_bus_details():
    """Get bus details for profile view."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    busSID = request.args.get('busSID')
    
    if not busSID:
        return jsonify({'bus': None}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT busSID, busID, busLicense, busType FROM `bus` WHERE busSID = %s', (busSID,))
        row = cursor.fetchone()
        if row:
            bus = {
                'busSID': int(row[0]),
                'busID': str(row[1]),
                'busLicense': str(row[2]),
                'busType': str(row[3])
            }
        else:
            bus = None
        cursor.close()
        return jsonify({'bus': bus})
    except Exception as e:
        app.logger.error('Error loading bus details: %s', e)
        cursor.close()
        return jsonify({'bus': None}), 500


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
@app.route('/analytics')
def analytics_page():
    if 'user_id' not in session:
        flash('Please log in first.')
        return redirect(url_for('index'))
    
    logged_in_user_id = session['user_id']
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Fetch the logged-in user's ID for display
        cursor.execute('SELECT userID FROM `user` WHERE userID = %s', (logged_in_user_id,))
        user_row = cursor.fetchone()
        user_id = user_row[0] if user_row else logged_in_user_id
    except Exception as e:
        app.logger.error('Error fetching analytics data: %s', e)
        user_id = logged_in_user_id
    finally:
        cursor.close()
    
    return render_template('analytics.html', user_id=user_id, current_date=datetime.now().strftime('%Y-%m-%d'), from_date=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))


@app.route('/api/analytics/transactions')
def api_analytics_transactions():
    """Return transaction data for OLAP analysis."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Build WHERE clause based on filters
        where_parts = []
        params = []
        
        from_date = request.args.get('from_date')
        to_date = request.args.get('to_date')
        bus_id = request.args.get('bus_id')
        customer_type = request.args.get('customer_type')
        user_id = request.args.get('user_id')
        search = request.args.get('search')
        
        if from_date:
            where_parts.append('d.dateID >= %s')
            params.append(from_date)
        if to_date:
            where_parts.append('d.dateID <= %s')
            params.append(to_date)
        if bus_id:
            where_parts.append('b.busSID = %s')
            params.append(bus_id)
        if customer_type:
            where_parts.append('c.custType = %s')
            params.append(customer_type)
        if user_id:
            where_parts.append('u.userID = %s')
            params.append(user_id)
        if search:
            search_condition = '''
                (t.refNo LIKE %s OR 
                 b.busID LIKE %s OR 
                 b.busType LIKE %s OR 
                 b.busLicense LIKE %s OR 
                 lf.city LIKE %s OR 
                 lf.landmark LIKE %s OR 
                 lt.city LIKE %s OR 
                 lt.landmark LIKE %s OR 
                 c.custType LIKE %s OR 
                 u.userID LIKE %s OR 
                 u.userFN LIKE %s OR 
                 u.userLN LIKE %s OR 
                 d.dateID LIKE %s OR 
                 d.year LIKE %s OR 
                 d.month LIKE %s)
            '''
            search_param = f'%{search}%'
            where_parts.append(search_condition)
            params.extend([search_param] * 15)
        
        where_clause = ' AND '.join(where_parts) if where_parts else '1=1'
        
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 100))
        offset = (page - 1) * limit
        
        # First get total count for pagination info
        count_query = f'''
            SELECT COUNT(*) as total
            FROM `transaction` t
            JOIN `date` d ON t.dateSID = d.dateSID
            JOIN `bus` b ON t.busSID = b.busSID
            JOIN `loc` lf ON t.locSIDfrom = lf.locSID
            JOIN `loc` lt ON t.locSIDto = lt.locSID
            JOIN `customer` c ON t.custSID = c.custSID
            JOIN `user` u ON t.userSID = u.userSID
            WHERE {where_clause}
        '''
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Fetch transaction data with pagination
        query = f'''
            SELECT 
                t.refNo,
                d.dateID as date,
                t.time,
                b.busID,
                b.busType,
                lf.city as from_location,
                lt.city as to_location,
                c.custType as customer_type,
                t.distance,
                t.price,
                t.qty,
                t.discount,
                t.totalSum as total,
                u.userID as user_id,
                u.userType as user_type
            FROM `transaction` t
            JOIN `date` d ON t.dateSID = d.dateSID
            JOIN `bus` b ON t.busSID = b.busSID
            JOIN `loc` lf ON t.locSIDfrom = lf.locSID
            JOIN `loc` lt ON t.locSIDto = lt.locSID
            JOIN `customer` c ON t.custSID = c.custSID
            JOIN `user` u ON t.userSID = u.userSID
            WHERE {where_clause}
            ORDER BY d.dateID DESC, t.time DESC
            LIMIT {limit} OFFSET {offset}
        '''
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        transactions = []
        for row in rows:
            transactions.append({
                'refNo': str(row[0]),
                'date': str(row[1]),
                'time': str(row[2]),
                'busID': str(row[3]),
                'busType': str(row[4]),
                'from_location': str(row[5]),
                'to_location': str(row[6]),
                'customer_type': str(row[7]),
                'distance': float(row[8]),
                'price': float(row[9]),
                'qty': int(row[10]),
                'discount': float(row[11]),
                'total': float(row[12]),
                'user_id': str(row[13]),
                'user_type': str(row[14])
            })
        
        cursor.close()
        return jsonify({
            'transactions': transactions,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': total_count,
                'total_pages': (total_count + limit - 1) // limit
            }
        })
        
    except Exception as e:
        app.logger.error('Error loading analytics transactions: %s', e)
        cursor.close()
        return jsonify({'transactions': [], 'error': str(e)}), 500

@app.route("/bi", methods=["GET", "POST"])
def bi_dashboard():
    if request.method == "POST":
        file = request.files.get("file")

        if file:
            df = pd.read_csv(file)

            # 1️⃣ Standardize column names
            df.columns = df.columns.str.strip().str.capitalize()

            # 2️⃣ Ensure 'From' and 'To' columns exist
            if "From" not in df.columns:
                df["From"] = "Unknown"
            if "To" not in df.columns:
                df["To"] = "Unknown"

            df["From"] = df["From"].fillna("Unknown").astype(str)
            df["To"] = df["To"].fillna("Unknown").astype(str)

            # 3️⃣ Create Route column
            df["Route"] = df["From"] + " → " + df["To"]

            # 4️⃣ Convert Quantity to numeric
            df["Quantity"] = pd.to_numeric(df.get("Quantity", pd.Series([0]*len(df))), errors='coerce').fillna(0)

            # 5️⃣ Handle Total column
            if "Total" not in df.columns:
                # Try to compute Total as Quantity * Price if Price exists
                if "Price" in df.columns:
                    df["Price"] = pd.to_numeric(df["Price"], errors='coerce').fillna(0)
                    df["Total"] = df["Quantity"] * df["Price"]
                else:
                    df["Total"] = df["Quantity"]  # fallback if no Price
            else:
                df["Total"] = pd.to_numeric(df["Total"], errors='coerce').fillna(0)

            # 6️⃣ Convert Time to datetime safely
            df["Time"] = pd.to_datetime(df.get("Time"), errors='coerce')
            df["Hour"] = df["Time"].dt.hour.fillna(-1).astype(int)

            # 7️⃣ Aggregate routes and hours safely
            route_total = df.groupby("Route")["Total"].sum()
            most_profitable_route = route_total.idxmax() if not route_total.empty and route_total.sum() > 0 else "N/A"

            route_quantity = df.groupby("Route")["Quantity"].sum()
            busiest_route = route_quantity.idxmax() if not route_quantity.empty and route_quantity.sum() > 0 else "N/A"

            hour_quantity = df.groupby("Hour")["Quantity"].sum()
            peak_hour = hour_quantity.idxmax() if not hour_quantity.empty and hour_quantity.sum() > 0 else "N/A"

            # 8️⃣ Prepare insights dictionary
            total_trips = len(df)
            total_passengers = df["Quantity"].sum()
            total_revenue = df["Total"].sum()
            avg_revenue = df["Total"].mean() if total_revenue > 0 else 0

            insights = {
                "total_trips": total_trips,
                "total_passengers": total_passengers,
                "total_revenue": total_revenue,
                "avg_revenue": avg_revenue,
                "top_route": most_profitable_route,
                "busiest_route": busiest_route,
                "peak_hour": peak_hour
            }

            # 9️⃣ Generate BI report
            report_path = generate_bi_report(insights)
            return send_file(report_path, as_attachment=True)

    return render_template("bi.html")

def generate_bi_report(insights):
    doc = Document()

    # Title
    doc.add_heading("BusMate Business Intelligence Report", level=1)
    doc.add_paragraph(f"Generated on {datetime.now().strftime('%B %d, %Y')}")

    # Executive Summary
    doc.add_heading("Executive Summary", level=2)
    doc.add_paragraph(
        "This report analyzes BusMate ticketing data to provide actionable "
        "business intelligence insights for management decision-making."
    )

    # Operational Overview
    total_revenue = insights.get('total_revenue', 0)
    avg_revenue = insights.get('avg_revenue', 0)
    total_trips = insights.get('total_trips', 0)
    total_passengers = insights.get('total_passengers', 0)

    total_revenue_str = f"₱{total_revenue:.2f}" if total_revenue > 0 else "₱0.00"
    avg_revenue_str = f"₱{avg_revenue:.2f}" if avg_revenue > 0 else "₱0.00"

    doc.add_heading("Operational Overview", level=2)
    doc.add_paragraph(
        f"A total of {total_trips} trips were recorded, carrying {int(total_passengers)} passengers "
        f"and generating {total_revenue_str} in revenue. Average revenue per trip: {avg_revenue_str}."
    )

    # Route & Demand Analysis
    top_route = insights.get('top_route', "N/A")
    busiest_route = insights.get('busiest_route', "N/A")
    peak_hour = insights.get('peak_hour', "N/A")

    # Handle peak_hour display
    if isinstance(peak_hour, int) and peak_hour >= 0:
        peak_hour_str = f"{peak_hour}:00 hours"
    else:
        peak_hour_str = "N/A"

    doc.add_heading("Route & Demand Analysis", level=2)
    doc.add_paragraph(
        f"The most profitable route is {top_route}, while the busiest route is {busiest_route}. "
        f"Peak demand occurs at {peak_hour_str}."
    )

    # Recommendations
    doc.add_heading("Recommendations", level=2)
    doc.add_paragraph(
        "Management is advised to allocate more buses during peak hours and "
        "prioritize high-performing routes to maximize revenue."
    )

    file_name = "BusMate_BI_Report.docx"
    doc.save(file_name)
    return file_name


if __name__ == '__main__':
    print(f'Starting Flask app with DB backend: {connector_backend}')
    # Ensure required tables exist
    with app.app_context():
        ensure_user_bus_assignment_table()
    app.run(host='0.0.0.0', port=5500, debug=True)
