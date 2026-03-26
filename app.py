import streamlit as st
import sqlite3
import hashlib
from datetime import datetime, date, timedelta
import os
import pandas as pd
from io import BytesIO
import time
import shutil

# =============================================
# DATABASE CONFIGURATION
# =============================================

DB_PATH = "petty_cash.db"


def get_db():
    """Get database connection - local SQLite only (safe for Streamlit Cloud free tier)"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA foreign_keys=ON")
            return conn
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            else:
                raise
    raise Exception("Could not connect to database after retries")


# =============================================
# DATABASE INITIALIZATION
# =============================================

def init_database():
    """Initialize database with complete structure"""
    conn = get_db()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS funds
                 (id INTEGER PRIMARY KEY,
                  name TEXT,
                  balance REAL,
                  department TEXT,
                  custodian TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS expenses
                 (id INTEGER PRIMARY KEY,
                  voucher_no TEXT UNIQUE,
                  date TEXT,
                  amount REAL,
                  description TEXT,
                  category TEXT,
                  status TEXT DEFAULT 'Pending',
                  created_by TEXT,
                  vendor TEXT,
                  paid_to TEXT,
                  actual_used REAL DEFAULT 0,
                  returned_amount REAL DEFAULT 0,
                  return_date TEXT,
                  return_reason TEXT,
                  approved_by TEXT,
                  approved_date TEXT,
                  rejected_reason TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (id INTEGER PRIMARY KEY,
                  username TEXT UNIQUE,
                  password TEXT,
                  role TEXT,
                  full_name TEXT,
                  department TEXT,
                  email TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS categories
                 (id INTEGER PRIMARY KEY,
                  name TEXT UNIQUE,
                  description TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS voucher_counter
                 (id INTEGER PRIMARY KEY,
                  last_voucher_no INTEGER)''')

    c.execute('''CREATE TABLE IF NOT EXISTS audit_log
                 (id INTEGER PRIMARY KEY,
                  timestamp TEXT,
                  username TEXT,
                  action TEXT,
                  details TEXT,
                  voucher_no TEXT)''')

    # Seed data
    c.execute("SELECT COUNT(*) FROM funds")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO funds (id, name, balance, department, custodian) VALUES (1, 'Main Petty Cash Fund', 100000.0, 'General', 'Finance Department')")

    c.execute("SELECT COUNT(*) FROM voucher_counter")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO voucher_counter (id, last_voucher_no) VALUES (1, 1000)")

    c.execute("SELECT COUNT(*) FROM categories")
    if c.fetchone()[0] == 0:
        default_categories = [
            ('Office Supplies', 'Pens, papers, stationery items'),
            ('Travel Expenses', 'Transportation, fuel, mileage'),
            ('Meals & Entertainment', 'Business meals, client entertainment'),
            ('Utilities', 'Electricity, water, internet bills'),
            ('Maintenance', 'Equipment repair and maintenance'),
            ('Postage & Courier', 'Shipping and delivery costs'),
            ('Emergency Expenses', 'Unexpected urgent costs'),
            ('Miscellaneous', 'Other general expenses'),
        ]
        for cat in default_categories:
            c.execute("INSERT INTO categories (name, description) VALUES (?, ?)", cat)

    c.execute("SELECT COUNT(*) FROM users")
    if c.fetchone()[0] == 0:
        for username, password, role, full_name, dept, email in [
            ('admin',   'admin123',   'admin',   'System Administrator', 'IT',          'admin@company.com'),
            ('manager', 'manager123', 'manager', 'Finance Manager',      'Finance',     'manager@company.com'),
            ('user',    'user123',    'user',    'Regular Employee',      'Operations',  'user@company.com'),
        ]:
            hashed = hashlib.sha256(password.encode()).hexdigest()
            c.execute("INSERT INTO users (username, password, role, full_name, department, email) VALUES (?,?,?,?,?,?)",
                      (username, hashed, role, full_name, dept, email))

    conn.commit()
    conn.close()


init_database()


# =============================================
# AUDIT LOG
# =============================================

def log_action(username, action, details="", voucher_no=""):
    """Write an audit log entry"""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "INSERT INTO audit_log (timestamp, username, action, details, voucher_no) VALUES (?,?,?,?,?)",
            (datetime.now().isoformat(sep=" ", timespec="seconds"), username, action, details, voucher_no)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass  # Never let audit failure break the app


# =============================================
# HELPERS
# =============================================

def format_currency(amount):
    if amount is None:
        return "Rs 0.00"
    try:
        return f"Rs {float(amount):,.2f}"
    except (ValueError, TypeError):
        return "Rs 0.00"


def login_user(username, password):
    conn = get_db()
    c = conn.cursor()
    hashed = hashlib.sha256(password.encode()).hexdigest()
    c.execute("SELECT * FROM users WHERE username=? AND password=?", (username, hashed))
    user = c.fetchone()
    conn.close()
    if user:
        return {'id': user[0], 'username': user[1], 'role': user[3],
                'full_name': user[4], 'department': user[5], 'email': user[6]}
    return None


def get_fund_balance(conn=None):
    """Get current fund balance. Accepts an existing connection for use inside transactions."""
    external = conn is not None
    if not external:
        conn = get_db()
    c = conn.cursor()
    c.execute("SELECT balance FROM funds WHERE id=1")
    result = c.fetchone()
    if not external:
        conn.close()
    return float(result[0]) if result else 0.0


# =============================================
# VOUCHER NUMBER — ATOMIC
# =============================================

def generate_voucher_number(conn):
    """
    Generate the next voucher number inside an existing connection/transaction.
    Caller must commit. Uses SELECT ... FOR UPDATE equivalent via immediate lock.
    """
    c = conn.cursor()
    c.execute("SELECT last_voucher_no FROM voucher_counter WHERE id=1")
    result = c.fetchone()
    last_no = result[0] if result else 1000
    new_no = last_no + 1
    voucher_no = f"PCV{new_no:06d}"
    c.execute("UPDATE voucher_counter SET last_voucher_no=? WHERE id=1", (new_no,))
    return voucher_no


# =============================================
# EXPENSE FUNCTIONS — ATOMIC TRANSACTIONS
# =============================================

def add_expense(amount, description, category, created_by, vendor="", paid_to=""):
    """
    Add a new expense. All DB writes happen in ONE transaction so a crash
    mid-way cannot leave balance and expenses out of sync.
    """
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")          # Exclusive write lock
        c = conn.cursor()

        # 1. Read balance inside the transaction
        c.execute("SELECT balance FROM funds WHERE id=1")
        row = c.fetchone()
        current_balance = float(row[0]) if row else 0.0

        if float(amount) > current_balance:
            conn.rollback()
            return False, "Insufficient funds in petty cash!", None

        # 2. Generate voucher number (inside same transaction)
        voucher_no = generate_voucher_number(conn)

        # 3. Insert expense
        c.execute(
            '''INSERT INTO expenses
               (voucher_no, date, amount, description, category, status,
                created_by, vendor, paid_to, actual_used, returned_amount)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
            (voucher_no, date.today().isoformat(), float(amount), description,
             category, 'Pending', created_by, vendor, paid_to, float(amount), 0.0)
        )

        # 4. Deduct balance
        new_balance = current_balance - float(amount)
        c.execute("UPDATE funds SET balance=? WHERE id=1", (new_balance,))

        conn.commit()
        log_action(created_by, "ADD_EXPENSE",
                   f"Amount: {format_currency(amount)}, Paid to: {paid_to}, Category: {category}",
                   voucher_no)
        return True, f"Expense submitted! Voucher: {voucher_no}", voucher_no

    except sqlite3.IntegrityError as e:
        conn.rollback()
        if "UNIQUE constraint failed" in str(e):
            return False, "Duplicate voucher number — please try again.", None
        return False, f"Database error: {str(e)}", None
    except Exception as e:
        conn.rollback()
        return False, f"Error: {str(e)}", None
    finally:
        conn.close()


def approve_expense(expense_id, approved_by):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        c = conn.cursor()
        c.execute("SELECT status, voucher_no FROM expenses WHERE id=?", (expense_id,))
        row = c.fetchone()
        if not row:
            conn.rollback()
            return False, "Expense not found"
        if row[0] != 'Pending':
            conn.rollback()
            return False, f"Expense is already {row[0]}"
        c.execute(
            "UPDATE expenses SET status='Approved', approved_by=?, approved_date=? WHERE id=?",
            (approved_by, date.today().isoformat(), expense_id)
        )
        conn.commit()
        log_action(approved_by, "APPROVE_EXPENSE", f"Expense ID: {expense_id}", row[1])
        return True, "Expense approved successfully"
    except Exception as e:
        conn.rollback()
        return False, f"Error: {str(e)}"
    finally:
        conn.close()


def reject_expense(expense_id, rejected_by, reason):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        c = conn.cursor()
        c.execute("SELECT status, amount, voucher_no FROM expenses WHERE id=?", (expense_id,))
        row = c.fetchone()
        if not row:
            conn.rollback()
            return False, "Expense not found"
        if row[0] != 'Pending':
            conn.rollback()
            return False, f"Expense is already {row[0]}"

        amount = float(row[1])

        # Refund the amount back to petty cash
        c.execute("SELECT balance FROM funds WHERE id=1")
        bal = c.fetchone()
        current_balance = float(bal[0]) if bal else 0.0
        c.execute("UPDATE funds SET balance=? WHERE id=1", (current_balance + amount,))

        c.execute(
            "UPDATE expenses SET status='Rejected', approved_by=?, approved_date=?, rejected_reason=? WHERE id=?",
            (rejected_by, date.today().isoformat(), reason, expense_id)
        )
        conn.commit()
        log_action(rejected_by, "REJECT_EXPENSE",
                   f"Expense ID: {expense_id}, Reason: {reason}, Refunded: {format_currency(amount)}", row[2])
        return True, f"Expense rejected. {format_currency(amount)} refunded to fund."
    except Exception as e:
        conn.rollback()
        return False, f"Error: {str(e)}"
    finally:
        conn.close()


def get_expenses(status_filter="All"):
    conn = get_db()
    c = conn.cursor()
    try:
        if status_filter == "All":
            c.execute("SELECT * FROM expenses ORDER BY date DESC, id DESC")
        else:
            c.execute("SELECT * FROM expenses WHERE status=? ORDER BY date DESC, id DESC", (status_filter,))
        return c.fetchall()
    except Exception:
        return []
    finally:
        conn.close()


def get_expense_stats():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT SUM(amount) FROM expenses WHERE status='Approved'")
        total_approved = float(c.fetchone()[0] or 0)
        c.execute("SELECT SUM(amount) FROM expenses WHERE status='Pending'")
        total_pending = float(c.fetchone()[0] or 0)
        c.execute("SELECT SUM(returned_amount) FROM expenses")
        total_returned = float(c.fetchone()[0] or 0)
        c.execute("SELECT status, COUNT(*) FROM expenses GROUP BY status")
        status_counts = dict(c.fetchall())
        return {
            'total_approved': total_approved,
            'total_pending': total_pending,
            'total_returned': total_returned,
            'status_counts': status_counts,
        }
    except Exception:
        return {'total_approved': 0, 'total_pending': 0, 'total_returned': 0, 'status_counts': {}}
    finally:
        conn.close()


# =============================================
# RETURN AMOUNT — ATOMIC
# =============================================

def return_unused_amount(expense_id, returned_amount, return_reason, returned_by):
    """Return unused cash — all writes in one transaction."""
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        c = conn.cursor()

        c.execute("SELECT amount, paid_to, returned_amount, status, voucher_no FROM expenses WHERE id=?", (expense_id,))
        row = c.fetchone()
        if not row:
            conn.rollback()
            return False, "Expense not found"

        original_amount = float(row[0])
        paid_to_person  = row[1]
        already_returned = float(row[2]) if row[2] else 0.0
        status          = row[3]
        voucher_no      = row[4]

        if status not in ('Approved', 'Pending'):
            conn.rollback()
            return False, "Can only return funds for Approved or Pending expenses"

        returned_amount_float = float(returned_amount)
        max_returnable = original_amount - already_returned

        if returned_amount_float <= 0:
            conn.rollback()
            return False, "Return amount must be greater than 0"
        if returned_amount_float > max_returnable:
            conn.rollback()
            return False, f"Cannot return more than {format_currency(max_returnable)}. Already returned: {format_currency(already_returned)}"

        new_returned_total = already_returned + returned_amount_float
        c.execute(
            "UPDATE expenses SET returned_amount=?, return_date=?, return_reason=? WHERE id=?",
            (new_returned_total, date.today().isoformat(), return_reason, expense_id)
        )

        c.execute("SELECT balance FROM funds WHERE id=1")
        bal = c.fetchone()
        current_balance = float(bal[0]) if bal else 0.0
        new_balance = current_balance + returned_amount_float
        c.execute("UPDATE funds SET balance=? WHERE id=1", (new_balance,))

        conn.commit()
        log_action(returned_by, "RETURN_FUNDS",
                   f"Returned: {format_currency(returned_amount_float)} from {paid_to_person}. New balance: {format_currency(new_balance)}",
                   voucher_no)
        return True, f"Successfully returned {format_currency(returned_amount_float)} from {paid_to_person}. New balance: {format_currency(new_balance)}"

    except Exception as e:
        conn.rollback()
        return False, f"Error: {str(e)}"
    finally:
        conn.close()


def get_returnable_expenses():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''
            SELECT id, voucher_no, date, amount, description, paid_to,
                   COALESCE(returned_amount, 0)
            FROM expenses
            WHERE status IN ('Approved','Pending')
              AND (returned_amount IS NULL OR returned_amount < amount)
            ORDER BY date DESC
        ''')
        return c.fetchall()
    except Exception:
        return []
    finally:
        conn.close()


def bulk_return_person_vouchers(paid_to_name, total_return_amount, return_reason, returned_by):
    """Distribute a bulk return across multiple vouchers for one person (oldest first)."""
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        c = conn.cursor()

        c.execute('''
            SELECT id, voucher_no, amount, COALESCE(returned_amount,0)
            FROM expenses
            WHERE paid_to=? AND status IN ('Approved','Pending')
            ORDER BY date ASC
        ''', (paid_to_name,))
        vouchers = c.fetchall()

        if not vouchers:
            conn.rollback()
            return False, "No vouchers found for this person"

        total_remaining = sum(float(v[2]) - float(v[3]) for v in vouchers if float(v[2]) - float(v[3]) > 0)
        return_float = float(total_return_amount)

        if return_float > total_remaining:
            conn.rollback()
            return False, f"Return amount ({format_currency(return_float)}) exceeds remaining balance ({format_currency(total_remaining)})"

        remaining_to_distribute = return_float
        processed = []

        for expense_id, voucher_no, amount, returned_amt in vouchers:
            if remaining_to_distribute <= 0:
                break
            remaining_on_voucher = float(amount) - float(returned_amt)
            if remaining_on_voucher <= 0:
                continue

            apply = min(remaining_to_distribute, remaining_on_voucher)
            new_returned = float(returned_amt) + apply
            c.execute(
                "UPDATE expenses SET returned_amount=?, return_date=?, return_reason=? WHERE id=?",
                (new_returned, date.today().isoformat(), f"Bulk return: {return_reason}", expense_id)
            )
            processed.append((voucher_no, apply))
            remaining_to_distribute -= apply

        # Update fund balance once
        c.execute("SELECT balance FROM funds WHERE id=1")
        bal = c.fetchone()
        current_balance = float(bal[0]) if bal else 0.0
        c.execute("UPDATE funds SET balance=? WHERE id=1", (current_balance + return_float,))

        conn.commit()
        log_action(returned_by, "BULK_RETURN",
                   f"Person: {paid_to_name}, Total: {format_currency(return_float)}, Vouchers: {len(processed)}")
        return True, f"Bulk return completed for {paid_to_name}: {format_currency(return_float)} returned across {len(processed)} voucher(s)"

    except Exception as e:
        conn.rollback()
        return False, f"Error in bulk return: {str(e)}"
    finally:
        conn.close()


def get_all_persons_with_pending_returns():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''
            SELECT DISTINCT paid_to FROM expenses
            WHERE status IN ('Approved','Pending')
              AND paid_to IS NOT NULL AND paid_to != ''
              AND (returned_amount IS NULL OR returned_amount < amount)
            ORDER BY paid_to
        ''')
        return [row[0] for row in c.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


def get_person_return_summary(paid_to_name):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''
            SELECT COUNT(*),
                   SUM(amount),
                   COALESCE(SUM(returned_amount),0),
                   SUM(amount) - COALESCE(SUM(returned_amount),0)
            FROM expenses
            WHERE paid_to=? AND status IN ('Approved','Pending')
        ''', (paid_to_name,))
        return c.fetchone() or (0, 0, 0, 0)
    except Exception:
        return (0, 0, 0, 0)
    finally:
        conn.close()


# =============================================
# FUND MANAGEMENT
# =============================================

def replenish_funds(amount, description, username):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        c = conn.cursor()
        c.execute("SELECT balance FROM funds WHERE id=1")
        bal = c.fetchone()
        current_balance = float(bal[0]) if bal else 0.0
        new_balance = current_balance + float(amount)
        c.execute("UPDATE funds SET balance=? WHERE id=1", (new_balance,))
        c.execute(
            '''INSERT INTO expenses
               (voucher_no, date, amount, description, category, status,
                created_by, vendor, paid_to, actual_used, returned_amount)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
            (f"FUND-{int(time.time())}", date.today().isoformat(), float(amount),
             f"Fund Replenishment: {description}", "Fund Addition", "Approved",
             username, "Internal", "Petty Cash Fund", 0.0, 0.0)
        )
        conn.commit()
        log_action(username, "REPLENISH_FUND", f"Added: {format_currency(amount)}, Reason: {description}")
        return True, f"Successfully added {format_currency(amount)} to petty cash!"
    except Exception as e:
        conn.rollback()
        return False, f"Error: {str(e)}"
    finally:
        conn.close()


def withdraw_funds(amount, description, username):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        c = conn.cursor()
        c.execute("SELECT balance FROM funds WHERE id=1")
        bal = c.fetchone()
        current_balance = float(bal[0]) if bal else 0.0
        if float(amount) > current_balance:
            conn.rollback()
            return False, "Insufficient funds!"
        new_balance = current_balance - float(amount)
        c.execute("UPDATE funds SET balance=? WHERE id=1", (new_balance,))
        c.execute(
            '''INSERT INTO expenses
               (voucher_no, date, amount, description, category, status,
                created_by, vendor, paid_to, actual_used, returned_amount)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
            (f"WD-{int(time.time())}", date.today().isoformat(), float(amount),
             f"Fund Withdrawal: {description}", "Fund Withdrawal", "Approved",
             username, "Internal", "Bank Deposit", 0.0, 0.0)
        )
        conn.commit()
        log_action(username, "WITHDRAW_FUND", f"Withdrew: {format_currency(amount)}, Reason: {description}")
        return True, f"Successfully withdrew {format_currency(amount)} from petty cash!"
    except Exception as e:
        conn.rollback()
        return False, f"Error: {str(e)}"
    finally:
        conn.close()


# =============================================
# USER MANAGEMENT
# =============================================

def get_all_users():
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id, username, role, full_name, department, email FROM users ORDER BY role, username")
    users = c.fetchall()
    conn.close()
    return users


def add_user(username, password, role, full_name, department, email, added_by="system"):
    conn = get_db()
    c = conn.cursor()
    try:
        hashed = hashlib.sha256(password.encode()).hexdigest()
        c.execute("INSERT INTO users (username, password, role, full_name, department, email) VALUES (?,?,?,?,?,?)",
                  (username, hashed, role, full_name, department, email))
        conn.commit()
        log_action(added_by, "ADD_USER", f"Created user: {username} ({role})")
        return True, "User created successfully!"
    except sqlite3.IntegrityError:
        return False, "Username already exists!"
    except Exception as e:
        return False, f"Error: {str(e)}"
    finally:
        conn.close()


def delete_user(user_id, deleted_by="system"):
    conn = get_db()
    c = conn.cursor()
    try:
        if st.session_state.user['id'] == user_id:
            return False, "Cannot delete your own account!"
        c.execute("SELECT username FROM users WHERE id=?", (user_id,))
        row = c.fetchone()
        username = row[0] if row else str(user_id)
        c.execute("DELETE FROM users WHERE id=?", (user_id,))
        conn.commit()
        log_action(deleted_by, "DELETE_USER", f"Deleted user: {username}")
        return True, "User deleted successfully!"
    except Exception as e:
        return False, f"Error: {str(e)}"
    finally:
        conn.close()


# =============================================
# CATEGORY MANAGEMENT
# =============================================

def get_categories():
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT name, description FROM categories ORDER BY name")
    cats = c.fetchall()
    conn.close()
    return cats


def add_category(name, description=""):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO categories (name, description) VALUES (?,?)", (name, description))
        conn.commit()
        return True, "Category added successfully!"
    except sqlite3.IntegrityError:
        return False, "Category already exists!"
    except Exception as e:
        return False, f"Error: {str(e)}"
    finally:
        conn.close()


def delete_category(name):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) FROM expenses WHERE category=?", (name,))
        count = c.fetchone()[0]
        if count > 0:
            return False, f"Cannot delete — category used in {count} expense(s)"
        c.execute("DELETE FROM categories WHERE name=?", (name,))
        conn.commit()
        return True, "Category deleted successfully!"
    except Exception as e:
        return False, f"Error: {str(e)}"
    finally:
        conn.close()


# =============================================
# BACKUP & EXPORT
# =============================================

def create_backup():
    try:
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"petty_cash_backup_{timestamp}.db")
        if os.path.exists(DB_PATH):
            shutil.copy2(DB_PATH, backup_file)
            return True, f"Backup created: {backup_file}"
        return False, "No database file found to backup"
    except Exception as e:
        return False, f"Backup failed: {str(e)}"


def export_to_excel():
    try:
        conn = get_db()
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for table in ['funds', 'expenses', 'users', 'categories', 'audit_log']:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                df.to_excel(writer, sheet_name=table, index=False)
            summary_df = pd.DataFrame({
                'Metric': ['Export Date', 'Total Expenses', 'Current Balance'],
                'Value': [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    len(pd.read_sql_query("SELECT * FROM expenses", conn)),
                    pd.read_sql_query("SELECT balance FROM funds WHERE id=1", conn)['balance'].iloc[0],
                ]
            })
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        output.seek(0)
        conn.close()
        return True, output
    except Exception as e:
        return False, f"Export failed: {str(e)}"


def import_from_excel(file):
    try:
        xls = pd.ExcelFile(file)
        conn = get_db()
        c = conn.cursor()
        allowed = ['funds', 'expenses', 'users', 'categories', 'voucher_counter']
        imported = []
        for table_name in xls.sheet_names:
            if table_name not in allowed:
                continue
            df = pd.read_excel(file, sheet_name=table_name)
            c.execute(f"DELETE FROM {table_name}")
            for _, row in df.iterrows():
                columns = ', '.join(row.index)
                placeholders = ', '.join(['?' for _ in range(len(row))])
                c.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", tuple(row.values))
            imported.append(table_name)
        conn.commit()
        conn.close()
        return True, f"Successfully imported: {', '.join(imported)}"
    except Exception as e:
        return False, f"Import failed: {str(e)}"


# =============================================
# REPORT HELPERS
# =============================================

def get_person_wise_report():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''
            SELECT created_by,
                   COUNT(*) as total_vouchers,
                   SUM(CAST(amount AS REAL)) as total_issued,
                   COALESCE(SUM(CAST(returned_amount AS REAL)),0) as total_returned,
                   SUM(CAST(amount AS REAL)) - COALESCE(SUM(CAST(returned_amount AS REAL)),0) as net_used
            FROM expenses WHERE status='Approved'
            GROUP BY created_by ORDER BY net_used DESC
        ''')
        return c.fetchall()
    except Exception:
        return []
    finally:
        conn.close()


def get_in_out_summary(start_date=None, end_date=None):
    conn = get_db()
    c = conn.cursor()
    try:
        if start_date and end_date:
            c.execute("SELECT SUM(CAST(amount AS REAL)) FROM expenses WHERE status='Approved' AND date BETWEEN ? AND ?", (start_date, end_date))
            money_out = float(c.fetchone()[0] or 0)
            c.execute("SELECT SUM(CAST(returned_amount AS REAL)) FROM expenses WHERE returned_amount > 0 AND date BETWEEN ? AND ?", (start_date, end_date))
            money_returned = float(c.fetchone()[0] or 0)
        else:
            c.execute("SELECT SUM(CAST(amount AS REAL)) FROM expenses WHERE status='Approved'")
            money_out = float(c.fetchone()[0] or 0)
            c.execute("SELECT SUM(CAST(returned_amount AS REAL)) FROM expenses WHERE returned_amount > 0")
            money_returned = float(c.fetchone()[0] or 0)
        return {'money_out': money_out, 'money_returned': money_returned, 'net_outflow': money_out - money_returned}
    except Exception:
        return {'money_out': 0, 'money_returned': 0, 'net_outflow': 0}
    finally:
        conn.close()


def get_daily_voucher_report(start_date, end_date):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('''
            SELECT date, COUNT(*) as total_vouchers,
                   SUM(CASE WHEN status='Approved' THEN amount ELSE 0 END) as total_expense,
                   SUM(CASE WHEN returned_amount > 0 THEN returned_amount ELSE 0 END) as total_returned
            FROM expenses WHERE date BETWEEN ? AND ?
            GROUP BY date ORDER BY date DESC
        ''', (start_date, end_date))
        return c.fetchall()
    except Exception:
        return []
    finally:
        conn.close()


def get_detailed_transactions_report(start_date=None, end_date=None):
    conn = get_db()
    c = conn.cursor()
    try:
        if start_date and end_date:
            c.execute('''
                SELECT voucher_no, date, amount, description, category, status,
                       created_by, vendor, paid_to, returned_amount, return_date, return_reason
                FROM expenses WHERE date BETWEEN ? AND ? ORDER BY date DESC, id DESC
            ''', (start_date, end_date))
        else:
            c.execute('''
                SELECT voucher_no, date, amount, description, category, status,
                       created_by, vendor, paid_to, returned_amount, return_date, return_reason
                FROM expenses ORDER BY date DESC, id DESC
            ''')
        return c.fetchall()
    except Exception:
        return []
    finally:
        conn.close()


def search_vouchers(search_term):
    conn = get_db()
    c = conn.cursor()
    try:
        pattern = f"%{search_term}%"
        c.execute('''
            SELECT * FROM expenses
            WHERE (voucher_no LIKE ? OR paid_to LIKE ? OR description LIKE ? OR vendor LIKE ?)
              AND status != 'Rejected'
            ORDER BY date DESC
        ''', (pattern, pattern, pattern, pattern))
        return c.fetchall()
    except Exception:
        return []
    finally:
        conn.close()


def get_all_paid_to_persons():
    conn = get_db()
    c = conn.cursor()
    c.execute('''
        SELECT paid_to, COUNT(*) as usage_count, SUM(amount) as total_amount,
               COALESCE(SUM(returned_amount),0) as total_returned, MAX(date) as last_used
        FROM expenses
        WHERE paid_to IS NOT NULL AND paid_to != '' AND status='Approved'
        GROUP BY paid_to ORDER BY usage_count DESC
    ''')
    persons = c.fetchall()
    conn.close()
    return persons


# =============================================
# VOUCHER HTML PRINTING
# =============================================

def create_printable_voucher_html(expense_data):
    idx = expense_data
    voucher_no   = idx[1] or "N/A"
    expense_date = idx[2] or "N/A"
    amount       = idx[3] or 0
    description  = idx[4] or "N/A"
    category     = idx[5] or "N/A"
    status       = idx[6] or "N/A"
    created_by   = idx[7] or "N/A"
    vendor       = idx[8] or "N/A"
    paid_to      = idx[9] or "N/A"

    return f"""<!DOCTYPE html>
<html>
<head>
  <title>Petty Cash Voucher - {voucher_no}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
    .voucher-container {{ border: 2px solid #000; padding: 25px; max-width: 800px; margin: 0 auto; background: white; }}
    .header {{ text-align: center; border-bottom: 2px solid #000; padding-bottom: 15px; margin-bottom: 20px; }}
    .details-table {{ width: 100%; border-collapse: collapse; margin-bottom: 25px; }}
    .details-table td {{ padding: 10px; border: 1px solid #ddd; vertical-align: top; }}
    .details-table td:first-child {{ font-weight: bold; width: 30%; background-color: #f8f9fa; }}
    .signature-area {{ margin-top: 40px; border-top: 2px solid #000; padding-top: 20px; }}
    .signature-table {{ width: 100%; }}
    .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #666; }}
    .amount {{ font-weight: bold; font-size: 18px; color: #2c3e50; }}
    @media print {{
      body {{ margin: 0; }}
      .no-print {{ display: none; }}
    }}
  </style>
</head>
<body>
  <div class="voucher-container">
    <div class="header">
      <h1>PETTY CASH VOUCHER</h1>
      <h3>Your Company Name</h3>
    </div>
    <table class="details-table">
      <tr><td>Voucher No:</td><td>{voucher_no}</td></tr>
      <tr><td>Date:</td><td>{expense_date}</td></tr>
      <tr><td>Amount:</td><td class="amount">{format_currency(amount)}</td></tr>
      <tr><td>Description:</td><td>{description}</td></tr>
      <tr><td>Category:</td><td>{category}</td></tr>
      <tr><td>Vendor:</td><td>{vendor}</td></tr>
      <tr><td>Paid To:</td><td>{paid_to}</td></tr>
      <tr><td>Submitted By:</td><td>{created_by}</td></tr>
      <tr><td>Status:</td><td>{status}</td></tr>
    </table>
    <div class="signature-area">
      <table class="signature-table">
        <tr>
          <td style="width:50%;text-align:center;">
            <div style="margin-top:60px;"><strong>Prepared By</strong><br><br>___________________<br>{created_by}</div>
          </td>
          <td style="width:50%;text-align:center;">
            <div style="margin-top:60px;"><strong>Approved By</strong><br><br>___________________<br>Finance Manager</div>
          </td>
        </tr>
      </table>
    </div>
    <div class="footer">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
  </div>
  <div class="no-print" style="text-align:center;margin-top:20px;padding:20px;">
    <button onclick="window.print()" style="padding:12px 24px;font-size:16px;background:#4CAF50;color:white;border:none;border-radius:5px;cursor:pointer;">🖨️ Print Voucher</button>
  </div>
</body>
</html>"""


def display_voucher(expense_data):
    voucher_no   = expense_data[1] or "N/A"
    expense_date = expense_data[2] or "N/A"
    amount       = expense_data[3] or 0
    description  = expense_data[4] or "N/A"
    category     = expense_data[5] or "N/A"
    status       = expense_data[6] or "N/A"
    created_by   = expense_data[7] or "N/A"
    vendor       = expense_data[8] or "N/A"
    paid_to      = expense_data[9] or "N/A"

    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<h1 style='text-align:center;color:#2c3e50;'>💰 PETTY CASH VOUCHER</h1>", unsafe_allow_html=True)
        st.markdown("<h3 style='text-align:center;color:#7f8c8d;'>Your Company Name</h3>", unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.info(f"**Voucher No:** {voucher_no}")
        st.info(f"**Date:** {expense_date}")
        st.info(f"**Amount:** {format_currency(amount)}")
        st.info(f"**Category:** {category}")
    with c2:
        st.info(f"**Description:** {description}")
        st.info(f"**Vendor:** {vendor}")
        st.info(f"**Paid To:** {paid_to}")
        st.info(f"**Status:** {status}")

    st.markdown("---")
    c3, c4 = st.columns(2)
    with c3:
        st.markdown("**Prepared By:**")
        st.write(f"**Name:** {created_by}")
        st.markdown("**Signature:** ___________________")
    with c4:
        st.markdown("**Approved By:**")
        st.write("**Name:** ___________________")
        st.markdown("**Signature:** ___________________")
    st.markdown("---")
    st.caption(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# =============================================
# UI PAGES
# =============================================

def show_dashboard():
    st.header("📊 Dashboard")

    balance = get_fund_balance()
    stats   = get_expense_stats()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 Fund Balance",     format_currency(balance))
    col2.metric("✅ Approved Expenses", format_currency(stats['total_approved']))
    col3.metric("🔄 Returned Amount",   format_currency(stats['total_returned']))
    pending_count = stats['status_counts'].get('Pending', 0)
    col4.metric("⏳ Pending Approvals", pending_count)

    st.subheader("📈 Financial Summary")
    sc1, sc2 = st.columns(2)
    with sc1:
        st.info(f"**Net Cash Outflow:** {format_currency(stats['total_approved'] - stats['total_returned'])}")
        st.info(f"**Available Balance:** {format_currency(balance)}")
        st.info(f"**Pending (on hold):** {format_currency(stats['total_pending'])}")
    with sc2:
        for status, count in stats['status_counts'].items():
            icon = "✅" if status == 'Approved' else "⏳" if status == 'Pending' else "❌"
            st.info(f"**{icon} {status}:** {count} voucher(s)")

    # Charts
    st.subheader("📊 Expense Breakdown by Category")
    conn = get_db()
    try:
        df_cat = pd.read_sql_query(
            "SELECT category, SUM(amount) as total FROM expenses WHERE status='Approved' GROUP BY category ORDER BY total DESC",
            conn
        )
        if not df_cat.empty:
            st.bar_chart(df_cat.set_index('category')['total'])
        else:
            st.info("No approved expenses yet.")

        st.subheader("📅 Daily Spend — Last 30 Days")
        df_daily = pd.read_sql_query(
            "SELECT date, SUM(amount) as total FROM expenses WHERE status='Approved' AND date >= date('now','-30 days') GROUP BY date ORDER BY date",
            conn
        )
        if not df_daily.empty:
            st.line_chart(df_daily.set_index('date')['total'])
        else:
            st.info("No data in last 30 days.")
    finally:
        conn.close()

    st.subheader("🕒 Recent Activity")
    expenses = get_expenses()
    if expenses:
        recent_data = []
        for exp in expenses[:10]:
            status_icon = "✅" if exp[6] == 'Approved' else "⏳" if exp[6] == 'Pending' else "❌"
            recent_data.append({
                'Voucher': exp[1], 'Date': exp[2],
                'Amount': format_currency(exp[3]),
                'Description': (exp[4][:35] + "...") if len(str(exp[4])) > 35 else exp[4],
                'Category': exp[5], 'By': exp[7],
                'Status': f"{status_icon} {exp[6]}"
            })
        st.dataframe(pd.DataFrame(recent_data), use_container_width=True, hide_index=True)
    else:
        st.info("📝 No vouchers recorded yet.")


def show_add_expense():
    st.header("➕ Add New Expense")

    categories       = [cat[0] for cat in get_categories()]
    paid_to_persons  = [p[0] for p in get_all_paid_to_persons()]

    with st.form("add_expense_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            amount = st.number_input("💵 Amount (PKR)", min_value=0.01, step=0.01, value=500.0, format="%.2f")
            category_option = st.selectbox("📁 Category", categories)
            add_new_cat = st.checkbox("Add new category inline")
            if add_new_cat:
                new_category = st.text_input("New Category Name")
                category = new_category if new_category else category_option
            else:
                category = category_option
            vendor = st.text_input("🏪 Vendor", placeholder="Store or service provider")

        with col2:
            description = st.text_area("📝 Description", placeholder="What was this expense for?", height=100)
            paid_to_option = st.selectbox("👤 Paid To (existing)", [""] + paid_to_persons)
            paid_to_custom = st.text_input("Or enter new name", placeholder="Who received the payment?")
            paid_to = paid_to_custom.strip() if paid_to_custom.strip() else paid_to_option

            current_balance = get_fund_balance()
            st.info(f"💰 Current balance: {format_currency(current_balance)}")
            if amount > current_balance:
                st.error(f"❌ Exceeds balance by {format_currency(amount - current_balance)}")

        submitted = st.form_submit_button("💾 Submit Expense", use_container_width=True)

    if submitted:
        if not description.strip():
            st.error("❌ Please enter a description")
        elif not paid_to:
            st.error("❌ Please enter who received the payment")
        else:
            success, message, voucher_no = add_expense(
                amount, description, category,
                st.session_state.user['full_name'],
                vendor, paid_to
            )
            if success:
                st.session_state.last_voucher = voucher_no
                st.session_state.show_voucher_printout = True
                st.success(f"✅ {message}")
                st.info(f"💰 Updated balance: {format_currency(get_fund_balance())}")
                st.balloons()
                st.rerun()
            else:
                st.error(f"❌ {message}")

    if st.session_state.get('show_voucher_printout') and st.session_state.get('last_voucher'):
        st.subheader("🎫 Voucher Printout")
        expenses    = get_expenses()
        new_expense = next((e for e in expenses if e[1] == st.session_state.last_voucher), None)
        if new_expense:
            tab1, tab2 = st.tabs(["📄 Quick View", "🖨️ Printable Version"])
            with tab1:
                display_voucher(new_expense)
            with tab2:
                html_content = create_printable_voucher_html(new_expense)
                st.components.v1.html(html_content, height=900, scrolling=True)
        if st.button("✖️ Close Voucher"):
            st.session_state.show_voucher_printout = False
            st.rerun()


def show_approval_workflow():
    """Manager/Admin: approve or reject pending expenses"""
    st.header("✅ Approval Workflow")

    if st.session_state.user['role'] not in ('admin', 'manager'):
        st.error("⛔ Manager or Administrator access required!")
        return

    pending = [e for e in get_expenses("Pending") if e[5] not in ("Fund Addition", "Fund Withdrawal")]

    if not pending:
        st.success("🎉 No pending expenses! All caught up.")
        return

    st.info(f"**{len(pending)} expense(s)** awaiting your approval.")

    for exp in pending:
        expense_id   = exp[0]
        voucher_no   = exp[1]
        expense_date = exp[2]
        amount       = exp[3]
        description  = exp[4]
        category     = exp[5]
        created_by   = exp[7]
        vendor       = exp[8]
        paid_to      = exp[9]

        with st.expander(f"🧾 {voucher_no} — {format_currency(amount)} — {paid_to} — {expense_date}"):
            c1, c2 = st.columns(2)
            with c1:
                st.write(f"**Submitted by:** {created_by}")
                st.write(f"**Paid to:** {paid_to}")
                st.write(f"**Amount:** {format_currency(amount)}")
                st.write(f"**Category:** {category}")
            with c2:
                st.write(f"**Date:** {expense_date}")
                st.write(f"**Description:** {description}")
                st.write(f"**Vendor:** {vendor or 'N/A'}")

            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("✅ Approve", key=f"approve_{expense_id}", use_container_width=True):
                    success, msg = approve_expense(expense_id, st.session_state.user['full_name'])
                    if success:
                        st.success(f"✅ {msg}")
                        st.rerun()
                    else:
                        st.error(f"❌ {msg}")
            with col_b:
                reject_reason = st.text_input("Rejection reason", key=f"reason_{expense_id}", placeholder="Required to reject")
                if st.button("❌ Reject", key=f"reject_{expense_id}", use_container_width=True):
                    if not reject_reason.strip():
                        st.error("❌ Please provide a rejection reason")
                    else:
                        success, msg = reject_expense(expense_id, st.session_state.user['full_name'], reject_reason)
                        if success:
                            st.success(f"✅ {msg}")
                            st.rerun()
                        else:
                            st.error(f"❌ {msg}")


def show_view_expenses():
    st.header("📋 View Expenses")

    col1, col2 = st.columns(2)
    with col1:
        status_filter = st.selectbox("🔍 Filter by Status", ["All", "Pending", "Approved", "Rejected"])
    with col2:
        if st.button("🔄 Refresh"):
            st.rerun()

    expenses = get_expenses(status_filter)

    if not expenses:
        st.info("📭 No vouchers found.")
        return

    st.subheader(f"📊 {len(expenses)} voucher(s)")

    for exp in expenses:
        returned_amount = exp[11] if len(exp) > 11 else 0
        with st.expander(f"{exp[1]} — {format_currency(exp[3])} — {exp[4]} — {exp[2]}"):
            c1, c2 = st.columns(2)
            with c1:
                st.write(f"**Voucher No:** {exp[1]}")
                st.write(f"**Amount:** {format_currency(exp[3])}")
                st.write(f"**Description:** {exp[4]}")
                st.write(f"**Category:** {exp[5]}")
                st.write(f"**Vendor:** {exp[8] or 'N/A'}")
            with c2:
                st.write(f"**Date:** {exp[2]}")
                icon = "🟢" if exp[6] == 'Approved' else "🟡" if exp[6] == 'Pending' else "🔴"
                st.write(f"**Status:** {icon} {exp[6]}")
                st.write(f"**Submitted by:** {exp[7]}")
                st.write(f"**Paid to:** {exp[9] or 'N/A'}")
                if returned_amount and float(returned_amount) > 0:
                    st.write(f"**Returned:** {format_currency(returned_amount)}")

            if st.button("🖨️ Print Voucher", key=f"print_{exp[0]}", use_container_width=True):
                html_content = create_printable_voucher_html(exp)
                st.components.v1.html(html_content, height=900, scrolling=True)


def show_return_management():
    st.header("🔄 Return Amount Management")

    tab1, tab2, tab3 = st.tabs(["Single Return", "Return History", "Bulk Return"])

    with tab1:
        st.subheader("Return Unused Amount")
        returnable = get_returnable_expenses()

        if not returnable:
            st.info("📭 No expenses available for return")
            return

        expense_options = {}
        for exp in returnable:
            expense_id, voucher_no, exp_date, amount, description, paid_to, returned_amt = exp
            remaining = float(amount) - float(returned_amt)
            label = f"{voucher_no} — {exp_date} — {paid_to} — {format_currency(amount)} (Remaining: {format_currency(remaining)})"
            expense_options[label] = exp

        selected_display  = st.selectbox("Select Expense", list(expense_options.keys()))
        selected_expense  = expense_options[selected_display]
        expense_id, voucher_no, exp_date, amount, description, paid_to, returned_amt = selected_expense

        st.info(f"**{voucher_no}** — Paid to: {paid_to}")
        c1, c2 = st.columns(2)
        with c1:
            st.write(f"**Date:** {exp_date}")
            st.write(f"**Original Amount:** {format_currency(amount)}")
        with c2:
            st.write(f"**Already Returned:** {format_currency(returned_amt)}")
            remaining = float(amount) - float(returned_amt)
            st.write(f"**Remaining:** {format_currency(remaining)}")

        with st.form("return_form"):
            return_amount = st.number_input("Amount to Return (PKR)", min_value=0.01, max_value=float(remaining), value=float(remaining), step=100.0)
            return_reason = st.text_area("Reason for Return", placeholder="Why are you returning this amount?", height=80)
            if st.form_submit_button("💸 Process Return", use_container_width=True):
                if not return_reason.strip():
                    st.error("❌ Please enter a return reason")
                else:
                    success, message = return_unused_amount(expense_id, return_amount, return_reason, st.session_state.user['full_name'])
                    if success:
                        st.success(f"✅ {message}")
                        st.balloons()
                        st.rerun()
                    else:
                        st.error(f"❌ {message}")

    with tab2:
        st.subheader("Return History")
        conn = get_db()
        c = conn.cursor()
        c.execute('''
            SELECT voucher_no, date, amount, paid_to, returned_amount, return_date, return_reason
            FROM expenses WHERE returned_amount > 0 ORDER BY return_date DESC
        ''')
        returns = c.fetchall()
        conn.close()

        if returns:
            for ret in returns:
                with st.expander(f"{ret[0]} — {ret[5]} — Returned: {format_currency(ret[4])}"):
                    c1, c2 = st.columns(2)
                    with c1:
                        st.write(f"**Voucher:** {ret[0]}")
                        st.write(f"**Original Date:** {ret[1]}")
                        st.write(f"**Original Amount:** {format_currency(ret[2])}")
                        st.write(f"**Paid To:** {ret[3]}")
                    with c2:
                        st.write(f"**Returned Amount:** {format_currency(ret[4])}")
                        st.write(f"**Return Date:** {ret[5]}")
                        st.write(f"**Reason:** {ret[6]}")
        else:
            st.info("📭 No return history found")

    with tab3:
        st.subheader("🔄 Bulk Return for Person")
        persons = get_all_persons_with_pending_returns()

        if not persons:
            st.info("📭 No persons with pending returns")
            return

        selected_person = st.selectbox("Select Person", persons)

        if selected_person:
            total_vouchers, total_issued, total_returned, pending_amount = get_person_return_summary(selected_person)

            st.info(f"**Person:** {selected_person}")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Vouchers", total_vouchers)
            c2.metric("Total Issued", format_currency(total_issued))
            c3.metric("Total Returned", format_currency(total_returned))
            c4.metric("Pending Return", format_currency(pending_amount))

            with st.form("bulk_return_form"):
                return_amount = st.number_input("Total Amount to Return (PKR)", min_value=0.01, max_value=float(pending_amount or 1), value=float(pending_amount or 0), step=100.0)
                return_reason = st.text_area("Reason", placeholder="Reason for bulk return", height=80)
                if st.form_submit_button("🚀 Process Bulk Return", use_container_width=True):
                    if not return_reason.strip():
                        st.error("❌ Please enter a reason")
                    else:
                        with st.spinner("Processing bulk return..."):
                            success, message = bulk_return_person_vouchers(selected_person, return_amount, return_reason, st.session_state.user['full_name'])
                        if success:
                            st.success(f"✅ {message}")
                            st.balloons()
                            st.rerun()
                        else:
                            st.error(f"❌ {message}")


def show_audit_log():
    st.header("📋 Audit Log")

    conn = get_db()
    try:
        df = pd.read_sql_query(
            "SELECT timestamp, username, action, details, voucher_no FROM audit_log ORDER BY id DESC LIMIT 500",
            conn
        )
    finally:
        conn.close()

    if df.empty:
        st.info("No audit entries yet.")
        return

    col1, col2 = st.columns(2)
    with col1:
        filter_user   = st.selectbox("Filter by User", ["All"] + sorted(df['username'].unique().tolist()))
    with col2:
        filter_action = st.selectbox("Filter by Action", ["All"] + sorted(df['action'].unique().tolist()))

    if filter_user != "All":
        df = df[df['username'] == filter_user]
    if filter_action != "All":
        df = df[df['action'] == filter_action]

    st.dataframe(df, use_container_width=True, hide_index=True)

    csv = df.to_csv(index=False)
    st.download_button("📥 Export Audit Log", data=csv, file_name=f"audit_log_{date.today()}.csv", mime="text/csv")


def show_fund_management():
    st.header("💳 Fund Management")

    if st.session_state.user['role'] not in ('admin', 'manager'):
        st.error("⛔ Manager or Administrator access required!")
        return

    current_balance = get_fund_balance()
    stats = get_expense_stats()

    c1, c2, c3 = st.columns(3)
    c1.metric("💰 Current Balance", format_currency(current_balance))
    c2.metric("📤 Total Expenses",  format_currency(stats['total_approved']))
    c3.metric("🔄 Total Returns",   format_currency(stats['total_returned']))

    st.markdown("---")

    tab1, tab2, tab3 = st.tabs(["💵 Add Funds", "📤 Withdraw Funds", "📊 Fund History"])

    with tab1:
        with st.form("replenish_form"):
            amount      = st.number_input("Amount to Add (PKR)", min_value=1.0, step=1000.0, value=50000.0)
            description = st.text_input("Reason", placeholder="Monthly replenishment, etc.")
            if st.form_submit_button("💰 Add Funds", use_container_width=True):
                success, message = replenish_funds(amount, description, st.session_state.user['full_name'])
                if success:
                    st.success(f"✅ {message}")
                    st.rerun()
                else:
                    st.error(f"❌ {message}")

    with tab2:
        with st.form("withdraw_form"):
            amount      = st.number_input("Amount to Withdraw (PKR)", min_value=1.0, step=1000.0, value=10000.0)
            description = st.text_input("Reason", placeholder="Bank deposit, etc.")
            if st.form_submit_button("📤 Withdraw Funds", use_container_width=True):
                success, message = withdraw_funds(amount, description, st.session_state.user['full_name'])
                if success:
                    st.success(f"✅ {message}")
                    st.rerun()
                else:
                    st.error(f"❌ {message}")

    with tab3:
        conn = get_db()
        c = conn.cursor()
        c.execute('''
            SELECT voucher_no, date, amount, description, created_by
            FROM expenses WHERE category IN ('Fund Addition','Fund Withdrawal')
            ORDER BY date DESC
        ''')
        transactions = c.fetchall()
        conn.close()
        if transactions:
            data = [{'Voucher': t[0], 'Date': t[1], 'Amount': format_currency(t[2]),
                     'Type': "➕ ADD" if "Addition" in t[3] else "📤 WITHDRAW",
                     'Description': t[3], 'User': t[4]} for t in transactions]
            st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
        else:
            st.info("No fund transactions found")


def show_user_management():
    st.header("👥 User Management")

    if st.session_state.user['role'] != 'admin':
        st.error("⛔ Administrator access required!")
        return

    tab1, tab2 = st.tabs(["📋 User List", "➕ Add New User"])

    with tab1:
        users = get_all_users()
        for user in users:
            user_id, username, role, full_name, department, email = user
            with st.expander(f"👤 {full_name} ({username}) — {role.title()}"):
                c1, c2 = st.columns([3, 1])
                with c1:
                    st.write(f"**Username:** {username}")
                    st.write(f"**Role:** {role.title()}")
                    st.write(f"**Department:** {department}")
                    st.write(f"**Email:** {email}")
                with c2:
                    if user_id != st.session_state.user['id']:
                        if st.button("🗑️ Delete", key=f"del_user_{user_id}", use_container_width=True):
                            success, message = delete_user(user_id, st.session_state.user['full_name'])
                            if success:
                                st.success(f"✅ {message}")
                                st.rerun()
                            else:
                                st.error(f"❌ {message}")
                    else:
                        st.info("👤 You")

    with tab2:
        with st.form("add_user_form"):
            c1, c2 = st.columns(2)
            with c1:
                new_username = st.text_input("👤 Username")
                new_password = st.text_input("🔒 Password", type="password")
                full_name    = st.text_input("📛 Full Name")
            with c2:
                email      = st.text_input("📧 Email")
                role       = st.selectbox("🎭 Role", ["user", "manager", "admin"])
                department = st.text_input("🏢 Department")
            if st.form_submit_button("💾 Create User", use_container_width=True):
                if all([new_username, new_password, full_name, department]):
                    success, message = add_user(new_username, new_password, role, full_name, department, email, st.session_state.user['full_name'])
                    if success:
                        st.success(f"✅ {message}")
                        st.rerun()
                    else:
                        st.error(f"❌ {message}")
                else:
                    st.error("❌ Please fill all required fields")


def show_category_management():
    st.header("📁 Category Management")

    c1, c2 = st.columns(2)

    with c1:
        with st.form("add_category_form"):
            st.write("**Add New Category**")
            new_category   = st.text_input("Category Name", placeholder="e.g., Software Subscription")
            category_desc  = st.text_input("Description", placeholder="Brief description")
            if st.form_submit_button("➕ Add Category", use_container_width=True):
                if new_category:
                    success, message = add_category(new_category, category_desc)
                    if success:
                        st.success(f"✅ {message}")
                        st.rerun()
                    else:
                        st.error(f"❌ {message}")
                else:
                    st.error("❌ Please enter category name")

    with c2:
        st.write("**Existing Categories**")
        for cat_name, description in get_categories():
            ca, cb = st.columns([3, 1])
            with ca:
                st.write(f"**{cat_name}**")
                if description:
                    st.caption(description)
            with cb:
                if st.button("🗑️", key=f"del_cat_{cat_name}"):
                    success, message = delete_category(cat_name)
                    if success:
                        st.success(f"✅ {message}")
                        st.rerun()
                    else:
                        st.error(f"❌ {message}")


def show_paid_to_management():
    st.header("👤 Paid To Management")

    tab1, tab2 = st.tabs(["📊 Person Summary", "📋 All Persons"])

    with tab1:
        persons = get_all_paid_to_persons()
        if persons:
            total_issued   = sum(float(p[2]) for p in persons)
            total_returned = sum(float(p[3]) for p in persons)
            c1, c2, c3 = st.columns(3)
            c1.metric("Total People",    len(persons))
            c2.metric("Total Issued",    format_currency(total_issued))
            c3.metric("Total Returned",  format_currency(total_returned))

            data = [{
                'Person': p[0], 'Vouchers': p[1],
                'Total Issued':   format_currency(p[2]),
                'Total Returned': format_currency(p[3]),
                'Net Balance':    format_currency(float(p[2]) - float(p[3])),
                'Last Used': p[4]
            } for p in persons]
            df = pd.DataFrame(data)
            st.dataframe(df, use_container_width=True, hide_index=True)

            csv = df.to_csv(index=False)
            st.download_button("📥 Export CSV", data=csv, file_name=f"person_report_{date.today()}.csv", mime="text/csv")
        else:
            st.info("No paid-to persons found.")

    with tab2:
        for person, count, total_amt, returned_amt, last_used in get_all_paid_to_persons():
            with st.expander(f"👤 {person}"):
                c1, c2 = st.columns(2)
                with c1:
                    st.write(f"**Total Vouchers:** {count}")
                    st.write(f"**Total Amount:** {format_currency(total_amt)}")
                    st.write(f"**Last Used:** {last_used}")
                with c2:
                    st.write(f"**Total Returned:** {format_currency(returned_amt)}")
                    net = float(total_amt) - float(returned_amt)
                    st.write(f"**Net Balance:** {format_currency(net)}")
                    if net > 0:
                        st.info(f"🔄 {format_currency(net)} pending return")


def show_advanced_reports():
    st.header("📈 Advanced Reports")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Person-Wise", "In/Out Summary", "Daily Report", "Excel Export", "Voucher Search"
    ])

    with tab1:
        report = get_person_wise_report()
        if report:
            for row in report:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Employee",    row[0])
                c2.metric("Vouchers",    row[1])
                c3.metric("Total Issued", format_currency(row[2]))
                c4.metric("Net Used",    format_currency(row[4]))
                st.write("---")
        else:
            st.info("No approved vouchers yet.")

    with tab2:
        c1, c2 = st.columns(2)
        with c1:
            start_date = st.date_input("Start Date", value=date.today() - timedelta(days=30))
        with c2:
            end_date = st.date_input("End Date", value=date.today())
        if st.button("Generate In/Out Report"):
            data = get_in_out_summary(start_date.isoformat(), end_date.isoformat())
            c1, c2, c3 = st.columns(3)
            c1.metric("💰 Money Out",       format_currency(data['money_out']))
            c2.metric("🔄 Money Returned",  format_currency(data['money_returned']))
            c3.metric("📈 Net Outflow",     format_currency(data['net_outflow']))
            chart_df = pd.DataFrame({'Category': ['Money Out', 'Money Returned', 'Net Outflow'],
                                     'Amount': [data['money_out'], data['money_returned'], data['net_outflow']]})
            st.bar_chart(chart_df.set_index('Category'))

    with tab3:
        c1, c2 = st.columns(2)
        with c1:
            start_d = st.date_input("Start Date", value=date.today() - timedelta(days=30), key="daily_start")
        with c2:
            end_d = st.date_input("End Date", value=date.today(), key="daily_end")
        if st.button("Generate Daily Report"):
            daily = get_daily_voucher_report(start_d.isoformat(), end_d.isoformat())
            if daily:
                data = [{'Date': d[0], 'Vouchers': d[1],
                         'Expense (Rs)': float(d[2] or 0),
                         'Returned (Rs)': float(d[3] or 0),
                         'Net (Rs)': float(d[2] or 0) - float(d[3] or 0)} for d in daily]
                st.dataframe(pd.DataFrame(data), use_container_width=True)
            else:
                st.info("No data for selected range.")

    with tab4:
        c1, c2 = st.columns(2)
        with c1:
            export_start = st.date_input("From", value=date.today() - timedelta(days=30), key="exp_start")
        with c2:
            export_end   = st.date_input("To",   value=date.today(), key="exp_end")
        if st.button("📥 Generate Excel Report", use_container_width=True):
            transactions = get_detailed_transactions_report(export_start.isoformat(), export_end.isoformat())
            if transactions:
                rows = [{'Voucher No': t[0], 'Date': t[1], 'Amount (Rs)': float(t[2] or 0),
                         'Description': t[3], 'Category': t[4], 'Status': t[5],
                         'Created By': t[6], 'Vendor': t[7], 'Paid To': t[8],
                         'Returned (Rs)': float(t[9] or 0), 'Return Date': t[10], 'Return Reason': t[11],
                         'Net (Rs)': float(t[2] or 0) - float(t[9] or 0)} for t in transactions]
                df = pd.DataFrame(rows)
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Transactions', index=False)
                    pd.DataFrame({'Metric': ['Transactions', 'Total Amount', 'Total Returned', 'Net Amount'],
                                  'Value': [len(transactions), format_currency(df['Amount (Rs)'].sum()),
                                            format_currency(df['Returned (Rs)'].sum()),
                                            format_currency(df['Net (Rs)'].sum())]
                                  }).to_excel(writer, sheet_name='Summary', index=False)
                output.seek(0)
                st.download_button("📥 Download Excel", data=output,
                                   file_name=f"petty_cash_{export_start}_to_{export_end}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True)
                st.success(f"✅ {len(transactions)} transactions ready")
                st.dataframe(df.head(10), use_container_width=True)
            else:
                st.warning("No transactions in selected range.")

    with tab5:
        search_term = st.text_input("Search Vouchers", placeholder="Voucher number, person, description, vendor…")
        if st.button("🔎 Search") and search_term:
            results = search_vouchers(search_term)
            if results:
                st.success(f"Found {len(results)} voucher(s)")
                for exp in results:
                    with st.expander(f"{exp[1]} — {exp[2]} — {format_currency(exp[3])} — {exp[4]}"):
                        c1, c2 = st.columns(2)
                        with c1:
                            st.write(f"**Voucher No:** {exp[1]}")
                            st.write(f"**Date:** {exp[2]}")
                            st.write(f"**Amount:** {format_currency(exp[3])}")
                            st.write(f"**Category:** {exp[5]}")
                        with c2:
                            st.write(f"**Status:** {exp[6]}")
                            st.write(f"**Created By:** {exp[7]}")
                            st.write(f"**Vendor:** {exp[8] or 'N/A'}")
                            st.write(f"**Paid To:** {exp[9] or 'N/A'}")
                        if st.button("🖨️ Print", key=f"print_s_{exp[0]}"):
                            st.components.v1.html(create_printable_voucher_html(exp), height=900, scrolling=True)
            else:
                st.warning(f"No results for '{search_term}'")


def show_backup_management():
    st.header("💾 Backup & Data Management")

    tab1, tab2, tab3, tab4 = st.tabs(["🔄 Manual Backup", "📤 Export Data", "📥 Import Data", "🗂️ Backup Files"])

    with tab1:
        if st.button("🔄 Create Manual Backup", use_container_width=True):
            with st.spinner("Creating backup..."):
                success, message = create_backup()
            if success:
                st.success(f"✅ {message}")
            else:
                st.error(f"❌ {message}")

        backup_dir = "backups"
        if os.path.exists(backup_dir):
            backups = sorted([f for f in os.listdir(backup_dir) if f.endswith('.db')], reverse=True)
            if backups:
                st.subheader("Recent Backups")
                for b in backups[:5]:
                    bp = os.path.join(backup_dir, b)
                    st.write(f"📁 {b} — {datetime.fromtimestamp(os.path.getmtime(bp)).strftime('%Y-%m-%d %H:%M')}")
            else:
                st.info("No backup files yet")

    with tab2:
        if st.button("📊 Export All Data to Excel", use_container_width=True):
            with st.spinner("Exporting..."):
                success, result = export_to_excel()
            if success:
                st.success("✅ Data exported!")
                st.download_button("📥 Download Excel",
                                   data=result,
                                   file_name=f"petty_cash_export_{date.today()}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True)
            else:
                st.error(f"❌ {result}")

    with tab3:
        st.warning("⚠️ This replaces existing data. Backup first!")
        uploaded = st.file_uploader("Choose Excel file", type=['xlsx'])
        if uploaded and st.button("🚀 Import Data", use_container_width=True):
            with st.spinner("Importing..."):
                success, message = import_from_excel(uploaded)
            if success:
                st.success(f"✅ {message}")
                st.rerun()
            else:
                st.error(f"❌ {message}")

    with tab4:
        backup_dir = "backups"
        if os.path.exists(backup_dir):
            backups = sorted([f for f in os.listdir(backup_dir) if f.endswith('.db')], reverse=True)
            if backups:
                for b in backups:
                    bp = os.path.join(backup_dir, b)
                    size = os.path.getsize(bp) / 1024
                    ca, cb, cc = st.columns([3, 1, 1])
                    with ca:
                        st.write(f"**{b}**")
                        st.caption(f"{size:.1f} KB — {datetime.fromtimestamp(os.path.getmtime(bp)).strftime('%Y-%m-%d %H:%M')}")
                    with cb:
                        with open(bp, 'rb') as f:
                            st.download_button("📥", data=f, file_name=b, key=f"dl_{b}", use_container_width=True)
                    with cc:
                        if st.button("🗑️", key=f"delbk_{b}", use_container_width=True):
                            os.remove(bp)
                            st.rerun()
            else:
                st.info("No backup files found")


# =============================================
# MAIN APPLICATION
# =============================================

def main():
    st.set_page_config(
        page_title="Petty Cash Management",
        page_icon="💰",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.markdown("""
    <style>
    .main-header { font-size: 2rem; color: #1f77b4; text-align: center; margin-bottom: 1rem; }
    </style>
    """, unsafe_allow_html=True)

    # Session state defaults
    for key, default in [('logged_in', False), ('user', None), ('last_voucher', None), ('show_voucher_printout', False)]:
        if key not in st.session_state:
            st.session_state[key] = default

    if not st.session_state.logged_in:
        st.markdown('<div class="main-header">💰 Petty Cash Management System</div>', unsafe_allow_html=True)
        _, col, _ = st.columns([1, 2, 1])
        with col:
            with st.form("login_form"):
                st.subheader("🔐 Login")
                username = st.text_input("👤 Username")
                password = st.text_input("🔒 Password", type="password")
                if st.form_submit_button("🚀 Login", use_container_width=True):
                    if username and password:
                        user = login_user(username, password)
                        if user:
                            st.session_state.logged_in = True
                            st.session_state.user = user
                            log_action(user['username'], "LOGIN", f"Role: {user['role']}")
                            st.rerun()
                        else:
                            st.error("❌ Invalid username or password")
                    else:
                        st.warning("⚠️ Please enter both username and password")
            with st.expander("ℹ️ Demo Credentials"):
                st.write("**Admin:** `admin` / `admin123`")
                st.write("**Manager:** `manager` / `manager123`")
                st.write("**User:** `user` / `user123`")
        return

    # Sidebar
    user = st.session_state.user
    st.sidebar.title(f"👋 {user['full_name']}")
    st.sidebar.write(f"**Role:** {user['role'].title()}")
    st.sidebar.write(f"**Dept:** {user['department']}")
    st.sidebar.write(f"**Balance:** {format_currency(get_fund_balance())}")
    st.sidebar.markdown("---")

    # Menu — role-aware
    if user['role'] == 'user':
        menu_options = ["📊 Dashboard", "➕ Add Expense", "📋 View Expenses", "🔄 Return Funds"]
    elif user['role'] == 'manager':
        menu_options = ["📊 Dashboard", "➕ Add Expense", "📋 View Expenses",
                        "✅ Approvals", "🔄 Return Funds", "📈 Reports",
                        "📁 Categories", "👤 Paid To", "💳 Funds", "💾 Backup"]
    else:  # admin
        menu_options = ["📊 Dashboard", "➕ Add Expense", "📋 View Expenses",
                        "✅ Approvals", "🔄 Return Funds", "📈 Reports",
                        "📁 Categories", "👤 Paid To", "💳 Funds",
                        "💾 Backup", "👥 Users", "📋 Audit Log"]

    choice = st.sidebar.selectbox("📱 Navigation", menu_options)
    st.sidebar.markdown("---")

    if st.sidebar.button("🚪 Logout", use_container_width=True):
        log_action(user['username'], "LOGOUT")
        for key in ['logged_in', 'user', 'last_voucher', 'show_voucher_printout']:
            st.session_state[key] = False if key == 'logged_in' else None
        st.rerun()

    st.sidebar.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
    if st.sidebar.button("🔄 Refresh", use_container_width=True):
        st.rerun()

    st.markdown('<div class="main-header">💰 Petty Cash Management System</div>', unsafe_allow_html=True)

    page_map = {
        "📊 Dashboard":    show_dashboard,
        "➕ Add Expense":  show_add_expense,
        "📋 View Expenses": show_view_expenses,
        "✅ Approvals":    show_approval_workflow,
        "🔄 Return Funds": show_return_management,
        "📈 Reports":      show_advanced_reports,
        "📁 Categories":   show_category_management,
        "👤 Paid To":      show_paid_to_management,
        "💳 Funds":        show_fund_management,
        "💾 Backup":       show_backup_management,
        "👥 Users":        show_user_management,
        "📋 Audit Log":    show_audit_log,
    }

    if choice in page_map:
        page_map[choice]()


if __name__ == "__main__":
    main()
