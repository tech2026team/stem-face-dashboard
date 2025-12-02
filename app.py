from flask import Flask, request, jsonify, send_file, redirect, url_for, flash, session, send_from_directory, make_response, render_template, g
import pandas as pd
from datetime import datetime, timedelta
import os
import io
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# Audit logging (replaces legacy TutorAnalytics)
from app.core.audit_logger import log_admin_action, get_audit_logs
import shifts
import logging
from app.auth.service import role_required
from permissions import Permission, PermissionManager, permission_required, permissions_required, role_required as new_role_required
from permission_middleware import permission_context, api_permission_required, require_data_access, audit_permission_action, get_user_capabilities
from app.auth.utils import USERS_FILE, hash_password
import simplejson as sjson
from supabase import create_client
from dotenv import load_dotenv
from app.core.routes import analytics_bp
from app.auth.routes import auth_bp

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'your-secret-key-here')

# Initialize permission middleware
from permission_middleware import init_permission_middleware
init_permission_middleware(app)



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Register scheduling blueprint (always available - WCOnline style)
try:
    from app.scheduling.routes import scheduling_bp
    app.register_blueprint(scheduling_bp, url_prefix='/scheduling')
    logger.info("Scheduling system enabled")
except ImportError as e:
    logger.warning(f"Scheduling blueprint not available: {e}")

# Register analytics blueprint (Charts & Calendar)
app.register_blueprint(analytics_bp)
logger.info("Analytics blueprint registered (Charts & Calendar enabled)")

# Register auth blueprint
app.register_blueprint(auth_bp)
logger.info("Auth blueprint registered")

# Register admin blueprint
try:
    from app.admin.routes import admin_bp
    app.register_blueprint(admin_bp, url_prefix='/admin')
    logger.info("Admin blueprint registered")
except ImportError as e:
    logger.warning(f"Admin blueprint not available: {e}")

# Global flag to track if app has been initialized (no demo seeding)
_app_initialized = False

def initialize_app_once():
    """Initialize the application once (no demo data, no auto-logger)."""
    global _app_initialized
    if not _app_initialized:
        try:
            _app_initialized = True
        except Exception as e:
            logger.error(f"Error initializing app: {e}")

# No mock users in production



# User management via auth_utils.USERS_FILE and auth_utils.hash_password

def ensure_users_file():
    """Ensure users file exists with proper structure"""
    os.makedirs(os.path.dirname(USERS_FILE), exist_ok=True)
    if not os.path.exists(USERS_FILE):
        users_df = pd.DataFrame(columns=[
            'user_id', 'email', 'full_name', 'role', 'created_at', 'last_login', 'active', 'password_hash'
        ])
        # Create default admin user
        default_admin = {
            'user_id': 'ADMIN001',
            'email': 'admin@example.com',
            'full_name': 'System Administrator',
            'role': 'admin',
            'created_at': datetime.now().isoformat(),
            'last_login': datetime.now().isoformat(),
            'active': True,
            'password_hash': hash_password('admin123')
        }
        users_df = pd.concat([users_df, pd.DataFrame([default_admin])], ignore_index=True)
        users_df.to_csv(USERS_FILE, index=False)

def load_users():
    """Load all users from CSV"""
    ensure_users_file()
    try:
        df = pd.read_csv(USERS_FILE)
        if not df.empty:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            df['last_login'] = pd.to_datetime(df['last_login'], errors='coerce')
        return df
    except Exception as e:
        print(f"Error loading users: {e}")
        return pd.DataFrame(columns=[
            'user_id', 'email', 'full_name', 'role', 'created_at', 'last_login', 'active', 'password_hash'
        ])

def get_current_user():
    """Get current user from session (supports Supabase and legacy CSV)"""
    # Supabase Auth: user info is stored in session['user']
    if 'user' in session:
        user = session['user']
        # Try to provide a unified user dict for frontend
        return {
            'user_id': user.get('id') or user.get('user_id'),
            'email': user.get('email'),
            'full_name': user.get('user_metadata', {}).get('full_name', '') if 'user_metadata' in user else '',
            'role': user.get('user_metadata', {}).get('role', 'tutor') if 'user_metadata' in user else 'tutor',
            'active': True
        }
    # Legacy CSV Auth fallback
    user_email = session.get('user_email')
    if not user_email:
        return None
    df = load_users()
    user_row = df[df['email'] == user_email]
    if not user_row.empty:
        return user_row.iloc[0].to_dict()
    return None

def send_email_notification(to_email, subject, message):
    """Send email notification (placeholder for SMTP integration)"""
    try:
        # This is a placeholder - in production, configure SMTP settings
        print(f"EMAIL NOTIFICATION TO: {to_email}")
        print(f"SUBJECT: {subject}")
        print(f"MESSAGE: {message}")
        
        # TODO: Implement actual SMTP integration
        # Example SMTP configuration:
        # import smtplib
        # from email.mime.text import MIMEText
        # from email.mime.multipart import MIMEMultipart
        # 
        # msg = MIMEMultipart()
        # msg['From'] = 'noreply@tutordashboard.com'
        # msg['To'] = to_email
        # msg['Subject'] = subject
        # msg.attach(MIMEText(message, 'plain'))
        # 
        # server = smtplib.SMTP('smtp.gmail.com', 587)
        # server.starttls()
        # server.login('your-email@gmail.com', 'your-app-password')
        # server.send_message(msg)
        # server.quit()
        
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False

def send_shift_alert_email(tutor_email, tutor_name, alert_type, details):
    """Send specific shift-related alert emails"""
    subject_map = {
        'late_checkin': f'Late Check-in Alert - {tutor_name}',
        'early_checkout': f'Early Check-out Alert - {tutor_name}',
        'short_shift': f'Short Shift Alert - {tutor_name}',
        'overlapping': f'Overlapping Shifts Alert - {tutor_name}',
        'missed_shift': f'Missed Shift Alert - {tutor_name}',
        'no_checkout': f'Missing Check-out Alert - {tutor_name}'
    }
    
    subject = subject_map.get(alert_type, f'Shift Alert - {tutor_name}')
    
    message = f"""
Dear {tutor_name},

This is an automated alert from the Tutor Dashboard regarding your shift:

ALERT TYPE: {alert_type.replace('_', ' ').title()}
DETAILS: {details}

Please review your schedule and take appropriate action.

Best regards,
Tutor Dashboard System
    """
    
    return send_email_notification(tutor_email, subject, message)



@app.route('/')
def index():
    """Serve landing page or dashboard based on authentication"""
    # Initialize app on first request
    initialize_app_once()
    
    # Check authentication
    user = get_current_user()
    if not user:
        # Show beautiful landing page for unauthenticated users
        return send_from_directory('templates', 'landing.html')
    
    # Authenticated users go to admin dashboard
    return redirect(url_for('admin.dashboard'))



@app.route('/admin/users')
@role_required('manager')
def admin_users():
    """Users management page – Manager/Admin only"""
    return render_template('admin_users.html', user=get_current_user())

@app.route('/admin/audit-logs')
@role_required('manager')
def admin_audit_logs():
    """Audit logs – Manager/Admin only"""
    return send_from_directory('templates', 'admin_audit_logs.html')

@app.route('/admin/shifts')
@role_required('lead_tutor')
def admin_shifts():
    """Shifts management – Lead Tutor and above"""
    return send_from_directory('templates', 'admin_shifts.html')



@app.route('/login')
def login_redirect():
    """Redirect legacy login route"""
    return redirect(url_for('auth.login'))

@app.route('/logout')
def logout_redirect():
    """Redirect legacy logout route"""
    return redirect(url_for('auth.logout'))

# API Endpoints

@app.route('/api/user-info')
def api_user_info():
    """Get current user information"""
    user = get_current_user()
    if not user:
        return jsonify({'error': 'Not authenticated'}), 401
    # Normalize session structure from auth.py (user_metadata)
    meta = user.get('user_metadata', {}) if isinstance(user, dict) else {}
    return jsonify({
        'user_id': user.get('id') or user.get('user_id') or meta.get('user_id'),
        'email': user.get('email'),
        'full_name': meta.get('full_name') or user.get('full_name'),
        'role': meta.get('role') or user.get('role'),
        'tutor_id': meta.get('tutor_id')
    })

@app.route('/api/dashboard-data')
def api_dashboard_data():
    """Get dashboard data - Updated to use SchedulingAnalytics (Phase 1 & 2)"""
    try:
        from app.core.analytics import SchedulingAnalytics
        from app.auth.service import filter_data_by_role, get_user_role, get_user_tutor_id
        
        # Use new SchedulingAnalytics instead of legacy TutorAnalytics
        analytics = SchedulingAnalytics(data_dir='data/core')
        
        # Apply role-based filtering if needed
        filters = {}
        try:
            role = get_user_role()
            tid = get_user_tutor_id()
            if role == 'tutor' and tid:
                # Filter appointments for this tutor only
                filters['tutor_ids'] = tid
        except Exception:
            pass
        
        # Get summary statistics (includes both new and legacy format for compatibility)
        summary = analytics.get_summary_stats(**filters)
        
        # Get logs for collapsible view (use appointments as logs)
        logs_for_collapsible_view = []
        try:
            # Convert recent appointments to log format for compatibility
            appointments_df = analytics.appointments.copy()
            if not appointments_df.empty and len(appointments_df) > 0:
                # Get last 50 appointments, sorted by date
                recent = appointments_df.sort_values('appointment_date', ascending=False).head(50)
                for _, apt in recent.iterrows():
                    log_entry = {
                        'date': str(apt['appointment_date']),
                        'tutor_id': apt.get('tutor_id', ''),
                        'tutor_name': '',
                        'student_name': apt.get('student_name', ''),
                        'start_time': str(apt.get('start_time', '')),
                        'end_time': str(apt.get('end_time', '')),
                        'status': apt.get('status', 'scheduled'),
                        'course_id': apt.get('course_id', '')
                    }
                    # Get tutor name
                    tutor = analytics.tutors[analytics.tutors['tutor_id'] == apt['tutor_id']]
                    if not tutor.empty:
                        log_entry['tutor_name'] = tutor.iloc[0].get('full_name', apt['tutor_id'])
                    logs_for_collapsible_view.append(log_entry)
        except Exception as e:
            logger.warning(f"Error generating logs view: {e}")
            logs_for_collapsible_view = []
        
        # Generate alerts based on scheduling data
        alerts = []
        try:
            # Check for pending confirmations
            if summary.get('pending_confirmations', 0) > 10:
                alerts.append({
                    'type': 'warning',
                    'title': 'High Pending Confirmations',
                    'message': f"{summary['pending_confirmations']} appointments are pending confirmation"
                })
            
            # Check for high cancellation rate
            total = summary.get('total_checkins', 0)
            cancelled = summary.get('cancelled_count', 0)
            if total > 0 and (cancelled / total) > 0.2:  # More than 20% cancelled
                alerts.append({
                    'type': 'danger',
                    'title': 'High Cancellation Rate',
                    'message': f"{(cancelled/total*100):.1f}% of appointments have been cancelled"
                })
            
            # Check for low active tutors
            if summary.get('active_tutors', 0) < 3:
                alerts.append({
                    'type': 'info',
                    'title': 'Low Tutor Activity',
                    'message': f"Only {summary['active_tutors']} active tutors this period"
                })
        except Exception as e:
            logger.warning(f"Error generating alerts: {e}")
            alerts = []
        
        return jsonify({
            'logs_for_collapsible_view': logs_for_collapsible_view,
            'summary': summary,
            'alerts': alerts
        })
    except Exception as e:
        print("DASHBOARD ERROR:", e)
        logger.error(f"Error getting dashboard data: {e}", exc_info=True)
        return jsonify({'error': 'Failed to load dashboard data'}), 500

@app.route('/dashboard-data')
def dashboard_data():
    """Alias for /api/dashboard-data for frontend compatibility"""
    return api_dashboard_data()

@app.route('/api/upcoming-shifts')
def api_upcoming_shifts():
    """Get upcoming shifts"""
    try:
        upcoming_shifts = shifts.get_upcoming_shifts()
        return jsonify(upcoming_shifts)
    except Exception as e:
        logger.error(f"Error getting upcoming shifts: {e}")
        return jsonify([])

# Admin API Endpoints

@app.route('/api/admin/users')
@permission_context
@api_permission_required(Permission.VIEW_USERS)
def api_admin_users():
    """Get users list based on permissions:
    - Users with VIEW_USERS permission can see users
    - Lead tutors see all users (read-only)
    - Regular tutors see only their own info
    """
    context = g.permission_context
    df = load_users()
    if 'password_hash' in df.columns:
        df = df.drop(columns=['password_hash'])
    for col in ['created_at', 'last_login']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else '')
    
    # Check if user can see all users or just their own
    if PermissionManager.has_permission(context.role, Permission.VIEW_ALL_DATA):
        users = df.fillna('').to_dict(orient='records')
        # Add read-only flag for lead tutors
        if context.role == 'lead_tutor':
            for u in users:
                u['read_only'] = True
        return jsonify(users)
    else:
        # Users can only see their own info
        user_row = df[df['email'] == context.user['email']]
        if user_row.empty:
            return jsonify([])
        return jsonify(user_row.fillna('').to_dict(orient='records'))

@app.route('/api/user/capabilities')
@permission_context
def api_user_capabilities():
    """Get current user's capabilities and permissions"""
    return jsonify(get_user_capabilities())

@app.route('/permission-management')
@permission_context
@api_permission_required(Permission.VIEW_USERS)
def permission_management():
    """Permission management page"""
    return render_template('permission_management.html')

@app.route('/api/admin/audit-logs')
@permission_context
@api_permission_required(Permission.VIEW_AUDIT_LOGS)
def api_audit_logs():
    """Get audit logs"""
    limit = request.args.get('limit', 100, type=int)
    event_type = request.args.get('event_type')
    user_email = request.args.get('user_email')
    severity = request.args.get('severity')
    
    try:
        from enhanced_audit import audit_logger, AuditEventType, AuditSeverity
        
        # Convert string parameters to enums if provided
        event_type_enum = None
        if event_type:
            try:
                event_type_enum = AuditEventType(event_type)
            except ValueError:
                pass
        
        severity_enum = None
        if severity:
            try:
                severity_enum = AuditSeverity(severity)
            except ValueError:
                pass
        
        logs = audit_logger.get_audit_logs(
            event_type=event_type_enum,
            user_email=user_email,
            severity=severity_enum,
            limit=limit
        )
        
        return jsonify(logs.to_dict('records'))
    except Exception as e:
        logger.error(f"Error retrieving audit logs: {e}")
        return jsonify({'error': 'Failed to retrieve audit logs'}), 500

@app.route('/api/admin/tutors')
@permission_context
@api_permission_required(Permission.MANAGE_SHIFTS)
def api_admin_tutors():
    """Get all tutors for shift assignment (admin/manager only)"""
    context = g.permission_context
    try:
        # Prefer Supabase users table if available
        if supabase:
            resp = supabase.table('users').select('user_id,email,full_name,role,active').in_('role', ['tutor','lead_tutor']).execute()
            records = resp.data or []
            return jsonify(records)
        # Fallback to CSV users file
        df = load_users()
        df = df[df['role'].isin(['tutor','lead_tutor'])]
        if 'password_hash' in df.columns:
            df = df.drop(columns=['password_hash'])
        return jsonify(df.fillna('').to_dict(orient='records'))
    except Exception as e:
        logger.error(f"Error fetching tutors: {e}")
        return jsonify([])

@app.route('/api/admin/shifts')
def api_admin_shifts():
    """Get all shifts for admin and manager only"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    try:
        # Get all shifts with assignments
        shifts_data = shifts.get_all_shifts_with_assignments()
        
        # Get upcoming shifts for the next 7 days
        upcoming_shifts_data = shifts.get_upcoming_shifts(days_ahead=7, page=1, per_page=50, exclude_today=False)
        
        return jsonify({
            'shifts': shifts_data,
            'upcoming_shifts': upcoming_shifts_data.get('shifts', [])
        })
    except Exception as e:
        logger.error(f"Error getting shifts: {e}")
        return jsonify({'error': 'Failed to load shifts'}), 500

@app.route('/api/admin/audit-logs')
def api_admin_audit_logs():
    """Get audit logs for admin, manager, and lead tutor (read-only for lead tutor)"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager', 'lead_tutor']:
        return jsonify({'error': 'Unauthorized'}), 403
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 25))
        logs_result = get_audit_logs(page, per_page)
        logs = logs_result.get('logs', [])
        total = logs_result.get('total', len(logs))
        total_pages = (total + per_page - 1) // per_page if per_page else 1
        response_data = {
            'logs': logs,
            'total': total,
            'pagination': {
                'total': total,
                'total_pages': total_pages,
                'page': page,
                'per_page': per_page
            }
        }
        print(f"[DEBUG] API Response: {len(logs)} logs, total: {total}, pages: {total_pages}")
        return app.response_class(
            response=sjson.dumps(response_data, ignore_nan=True),
            status=200,
            mimetype='application/json'
        )
    except Exception as e:
        logger.error(f"Error getting audit logs: {e}")
        return jsonify({'error': 'Failed to load audit logs'}), 500

# Admin POST endpoints

@app.route('/api/admin/create-user', methods=['POST'])
def api_admin_create_user():
    """Create a new user (admin/manager only)"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    data = request.get_json()
    if not data.get('password'):
        return jsonify({'error': 'Password is required'}), 400
    df = load_users()
    if data['email'] in df['email'].values:
        return jsonify({'error': 'User already exists'}), 400

    # Check if email already exists in Supabase Auth before creating
    if supabase:
        try:
            # Query Supabase Auth for existing user by email
            existing_user = None
            try:
                # This requires service_role key usually
                existing_user_resp = supabase.auth.admin.list_users(email=data['email'])
                if hasattr(existing_user_resp, 'users') and existing_user_resp.users:
                    existing_user = existing_user_resp.users[0]
            except Exception as check_e:
                logger.warning(f"Supabase Auth check failed (likely permissions): {check_e}")
            
            if existing_user:
                return jsonify({'error': 'A user with this email address already exists in Supabase Auth.'}), 400
                
            # Try to create in Supabase Auth
            try:
                response = supabase.auth.admin.create_user({
                    "email": data['email'],
                    "password": data['password'],
                    "user_metadata": {
                        "role": data['role'],
                        "full_name": data['full_name']
                    },
                    "email_confirm": True
                })
                
                # Check for success
                if not getattr(response, 'user', None):
                    logger.warning(f"Supabase Auth create failed: {response}")
                    # Don't return 400 here, fall back to CSV
                else:
                    # If Auth create succeeded, try to insert into users table
                    try:
                        supabase.table("users").insert({
                            "email": data['email'],
                            "role": data['role'],
                            "full_name": data['full_name'],
                            "created_at": datetime.now().isoformat()
                        }).execute()
                    except Exception as db_e:
                        logger.error(f"[Supabase DB] Failed to insert user into users table: {db_e}")
                        
            except Exception as create_e:
                logger.warning(f"Could not create user in Supabase Auth (likely permissions): {create_e}")
                # Fallback to CSV
                
        except Exception as e:
            logger.error(f"Supabase Auth exception: {e}")
            # Fallback to CSV
    else:
        # Supabase not configured, proceed to CSV
        pass

    # 2. Add to users.csv as before
    new_user = {
        'user_id': f"U{int(datetime.now().timestamp())}",
        'email': data['email'],
        'full_name': data['full_name'],
        'role': data['role'],
        'created_at': datetime.now().isoformat(),
        'last_login': '',
        'active': data.get('active', True),
        'password_hash': hash_password(data['password'])
    }
    df = pd.concat([df, pd.DataFrame([new_user])], ignore_index=True)
    df.to_csv(USERS_FILE, index=False)
    user_email = user.get('email', 'system') if user else 'system'
    log_admin_action('create_user', target_user_email=data.get('email'), details=f"Created user with role {data.get('role')}", user_email=user_email)
    return jsonify({'message': 'User created successfully'})

@app.route('/api/admin/edit-user', methods=['POST'])
def api_admin_edit_user():
    """Edit a user (admin/manager only, or tutor editing own info)"""
    user = get_current_user()
    data = request.get_json()
    df = load_users()
    idx = df.index[df['user_id'] == data['user_id']]
    if len(idx) == 0:
        return jsonify({'error': 'User not found'}), 404
    i = idx[0]
    # Admin/manager can edit anyone
    if user and user['role'] in ['admin', 'manager']:
        df.at[i, 'email'] = data['email']
        df.at[i, 'full_name'] = data['full_name']
        df.at[i, 'role'] = data['role']
        df.at[i, 'active'] = data.get('active', True)
        if data.get('password'):
            df.at[i, 'password_hash'] = hash_password(data['password'])
        df.to_csv(USERS_FILE, index=False)
        user_email = user.get('email', 'system') if user else 'system'
        log_admin_action('edit_user', target_user_email=data.get('email'), details=f"Edited user info for {data.get('user_id')}", user_email=user_email)
        return jsonify({'message': 'User updated successfully'})
    # Tutor can only edit their own info (password, maybe name)
    elif user and user['role'] == 'tutor' and df.at[i, 'email'] == user['email']:
        if data.get('full_name'):
            df.at[i, 'full_name'] = data['full_name']
        if data.get('password'):
            df.at[i, 'password_hash'] = hash_password(data['password'])
        df.to_csv(USERS_FILE, index=False)
        user_email = user.get('email', 'system') if user else 'system'
        log_admin_action('edit_user', target_user_email=data.get('email'), details=f"Tutor edited own info for {data.get('user_id')}", user_email=user_email)
        return jsonify({'message': 'User updated successfully'})
    else:
        return jsonify({'error': 'Unauthorized'}), 403

@app.route('/api/admin/delete-user', methods=['POST'])
def api_admin_delete_user():
    """Delete a user (admin only)"""
    user = get_current_user()
    if not user or user['role'] != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    data = request.get_json()
    df = load_users()
    idx = df.index[df['user_id'] == data['user_id']]
    if len(idx) == 0:
        return jsonify({'error': 'User not found'}), 404
    email = df.at[idx[0], 'email']
    df = df.drop(idx)
    df.to_csv(USERS_FILE, index=False)
    user_email = user.get('email', 'system') if user else 'system'
    log_admin_action('delete_user', target_user_email=email, details=f"Deleted user", user_email=user_email)
    return jsonify({'message': 'User deleted successfully'})

@app.route('/api/admin/change-role', methods=['POST'])
@permission_context
@api_permission_required(Permission.CHANGE_USER_ROLES)
@audit_permission_action("CHANGE_USER_ROLE")
def api_admin_change_role():
    """Change user role"""
    context = g.permission_context
    
    data = request.get_json()
    user_id = data.get('user_id')
    new_role = data.get('role')
    if not user_id or not new_role:
        return jsonify({'error': 'Missing user_id or role'}), 400

    # Validate role
    valid_roles = ['tutor', 'lead_tutor', 'manager', 'admin']
    if new_role not in valid_roles:
        return jsonify({'error': 'Invalid role'}), 400

    # Load current user data
    import pandas as pd
    csv_path = 'logs/users.csv'
    df = pd.read_csv(csv_path)
    
    # Find target user
    target_user = df[df['user_id'].astype(str) == str(user_id)]
    if target_user.empty:
        return jsonify({'error': 'User not found'}), 404
    
    target_user = target_user.iloc[0]
    old_role = target_user['role']
    target_email = target_user['email']
    
    # Prevent changing your own role
    if target_email == user['email']:
        return jsonify({'error': 'You cannot change your own role'}), 400
    
    # Prevent demoting the last admin
    if old_role == 'admin' and new_role != 'admin':
        admin_count = len(df[df['role'] == 'admin'])
        if admin_count <= 1:
            return jsonify({'error': 'Cannot demote the last admin user'}), 400
    
    # Prevent managers from promoting to admin (only admins can create other admins)
    if user['role'] == 'manager' and new_role == 'admin':
        return jsonify({'error': 'Managers cannot promote users to admin'}), 403
    
    # Update CSV
    df.loc[df['user_id'].astype(str) == str(user_id), 'role'] = new_role
    df.to_csv(csv_path, index=False)

    # Update Supabase users table
    if supabase:
        try:
            supabase.table('users').update({'role': new_role}).eq('user_id', user_id).execute()
        except Exception as e:
            print(f"[Supabase DB] Failed to update user role: {e}")

    # Log admin action with more details
    details = f"Changed role from {old_role} to {new_role} for user {target_email}"
    log_admin_action('change_role', target_user_email=target_email, details=details, user_email=user.get('email', 'system'))
    
    return jsonify({
        'message': 'Role updated successfully',
        'old_role': old_role,
        'new_role': new_role,
        'user_name': target_user['full_name']
    })

@app.route('/api/admin/create-shift', methods=['POST'])
def api_admin_create_shift():
    """Create a new shift"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    data = request.get_json()
    # In a real app, you would save to database
    return jsonify({'message': 'Shift created successfully'})

@app.route('/api/admin/assign-shift', methods=['POST'])
def api_admin_assign_shift():
    """Assign tutor to shift"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    data = request.get_json()
    # In a real app, you would save to database
    return jsonify({'message': 'Tutor assigned successfully'})

@app.route('/api/admin/activate-shift', methods=['POST'])
def api_admin_activate_shift():
    """Activate a shift"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    data = request.get_json()
    # In a real app, you would update database
    return jsonify({'message': 'Shift activated successfully'})

@app.route('/api/admin/deactivate-shift', methods=['POST'])
def api_admin_deactivate_shift():
    """Deactivate a shift"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    data = request.get_json()
    # In a real app, you would update database
    return jsonify({'message': 'Shift deactivated successfully'})

@app.route('/api/admin/populate-audit-logs', methods=['POST'])
def api_admin_populate_audit_logs():
    """Populate sample audit logs"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    # In a real app, you would add sample data to database
    return jsonify({'message': 'Sample audit logs added successfully'})

@app.route('/api/admin/delete-supabase-user', methods=['POST'])
def api_admin_delete_supabase_user():
    """Delete a user from Supabase Auth and optionally from the users table (admin/manager only)"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    data = request.get_json()
    email = data.get('email')
    if not email:
        return jsonify({'error': 'Email is required'}), 400
    if not supabase:
        return jsonify({'error': 'Supabase not configured'}), 500
    try:
        # Find user in Supabase Auth
        user_resp = supabase.auth.admin.list_users(email=email)
        if hasattr(user_resp, 'users') and user_resp.users:
            user_id = user_resp.users[0].id
            supabase.auth.admin.delete_user(user_id)
            # Optionally, also remove from users table
            try:
                supabase.table("users").delete().eq("email", email).execute()
            except Exception as db_e:
                print(f"[Supabase DB] Failed to delete user from users table: {db_e}")
            return jsonify({'message': f'User {email} deleted from Supabase Auth and users table.'})
        else:
            return jsonify({'error': 'User not found in Supabase Auth.'}), 404
    except Exception as e:
        print(f"Supabase Auth delete exception: {e}")
        return jsonify({'error': f'Could not delete user from Supabase Auth: {e}'}), 400

@app.route('/api/admin/user-activate', methods=['POST'])
def api_admin_user_activate():
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    data = request.get_json()
    email = data.get('email')
    active = data.get('active')
    if not email or active is None:
        return jsonify({'error': 'Missing email or active'}), 400
    # Update CSV
    import pandas as pd
    csv_path = 'data/core/users.csv'
    df = pd.read_csv(csv_path)
    if email not in df['email'].values:
        return jsonify({'error': 'User not found in CSV'}), 404
    df.loc[df['email'] == email, 'active'] = bool(active)
    df.to_csv(csv_path, index=False)
    # Update Supabase users table
    if supabase:
        try:
            supabase.table('users').update({'active': bool(active)}).eq('email', email).execute()
        except Exception as e:
            print(f"[Supabase DB] Failed to update user active status: {e}")
    # Log audit
    log_admin_action('user_activate', target_user_email=email, details=f"Set active={active}", user_email=user.get('email', 'system'))
    return jsonify({'success': True})

# Authentication endpoint




@app.route('/upcoming-shifts')
def upcoming_shifts():
    try:
        from shifts import get_upcoming_shifts
        
        # Get pagination parameters from query string
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 12, type=int)
        exclude_today = request.args.get('exclude_today', 'true').lower() == 'true'
        
        # Get upcoming shifts for the next 7 days with pagination
        result = get_upcoming_shifts(days_ahead=7, page=page, per_page=per_page, exclude_today=exclude_today)
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in /upcoming-shifts: {e}")
        return jsonify({'shifts': [], 'pagination': {}}), 500









@app.route('/api/lead-tutor/users')
def api_lead_tutor_users():
    """Get only the current user's info for lead_tutor role (future self-service)"""
    user = get_current_user()
    if not user or user['role'] != 'lead_tutor':
        return jsonify({'error': 'Unauthorized'}), 403
    df = load_users()
    user_row = df[df['email'] == user['email']]
    if user_row.empty:
        return jsonify({'error': 'User not found'}), 404
    user_info = user_row.drop(columns=['password_hash']).fillna('').to_dict(orient='records')[0]
    return jsonify(user_info)

@app.route('/api/tutor/user')
def api_tutor_user():
    """Get only the current tutor's info (for profile/self-service)"""
    user = get_current_user()
    if not user or user['role'] != 'tutor':
        return jsonify({'error': 'Unauthorized'}), 403
    df = load_users()
    user_row = df[df['email'] == user['email']]
    if user_row.empty:
        return jsonify({'error': 'User not found'}), 404
    user_info = user_row.drop(columns=['password_hash']).fillna('').to_dict(orient='records')[0]
    return jsonify(user_info)

@app.route('/profile')
def profile():
    user = get_current_user()
    if not user:
        return redirect(url_for('login'))
    return render_template('profile.html', user=user)

@app.route('/api/profile', methods=['GET'])
def api_profile_get():
    user = get_current_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    return jsonify(user)

@app.route('/api/profile', methods=['POST'])
def api_profile_update():
    user = get_current_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json()
    updated = False
    # Update full_name in session and persistent storage
    if 'full_name' in data and data['full_name']:
        new_name = data['full_name'].strip()
        if new_name and new_name != user.get('full_name', ''):
            # Update in session
            session_user = session.get('user', {})
            if 'user_metadata' in session_user:
                session_user['user_metadata']['full_name'] = new_name
            session['user'] = session_user
            updated = True
            # Update in Supabase custom users table if available
            try:
                if supabase:
                    supabase.table('users').update({'full_name': new_name}).eq('email', user['email']).execute()
                    # Also update Supabase Auth user_metadata so next login has the new name
                    try:
                        user_id = session_user.get('id') or user.get('user_id')
                        if user_id:
                            supabase.auth.admin.update_user_by_id(user_id, {"user_metadata": {"full_name": new_name}})
                    except Exception as e:
                        logger.warning(f"Supabase Auth full_name update failed: {e}")
            except Exception as e:
                logger.warning(f"Supabase full_name update failed: {e}")
            # Update in local CSV users file if present
            try:
                import pandas as pd
                if os.path.exists(USERS_FILE):
                    df = pd.read_csv(USERS_FILE)
                    if 'email' in df.columns and 'full_name' in df.columns:
                        df.loc[df['email'] == user['email'], 'full_name'] = new_name
                        df.to_csv(USERS_FILE, index=False)
            except Exception as e:
                logger.warning(f"CSV full_name update failed: {e}")
    # Update password
    if 'password' in data and data['password']:
        # Update in Supabase Auth if enabled, else CSV
        # ... update logic ...
        updated = True
    if updated:
        # Log audit
        try:
            log_admin_action('update_profile', target_user_email=user['email'], details='Updated profile fields', user_email=user.get('email', 'system'))
        except Exception:
            pass
        # Return fresh session user so UI reflects immediately
        return jsonify({'success': True, 'user': session.get('user')})
    return jsonify({'success': False, 'error': 'No changes'})

@app.route('/api/dashboard-alerts')
def api_dashboard_alerts():
    """Return dashboard alerts for the current user (or all users if admin/manager)"""
    user = get_current_user()
    if not user:
        return jsonify({'alerts': []})

    # Legacy face log system removed - using appointment-based alerts instead
    # Alerts are now generated from appointments data in the dashboard-data endpoint
    alerts = []
    
    # Return empty alerts - alerts are now handled by SchedulingAnalytics
    # in the /api/dashboard-data endpoint using appointment data
    return jsonify({'alerts': alerts})

@app.route('/api/notification-settings', methods=['GET'])
def api_notification_settings():
    """Get notification settings for the current user"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    # Default notification settings
    settings = {
        'email_notifications': True,
        'late_checkin_alerts': True,
        'early_checkout_alerts': True,
        'short_shift_alerts': True,
        'overlapping_shift_alerts': True,
        'missing_checkout_alerts': True,
        'smtp_configured': False,  # Will be True when SMTP is properly configured
        'notification_email': user['email']
    }
    
    return jsonify(settings)

@app.route('/api/notification-settings', methods=['POST'])
def api_update_notification_settings():
    """Update notification settings"""
    user = get_current_user()
    if not user or user['role'] not in ['admin', 'manager']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    data = request.get_json()
    
    # In a real implementation, you would save these settings to a database
    # For now, we'll just return success
    updated_settings = {
        'email_notifications': data.get('email_notifications', True),
        'late_checkin_alerts': data.get('late_checkin_alerts', True),
        'early_checkout_alerts': data.get('early_checkout_alerts', True),
        'short_shift_alerts': data.get('short_shift_alerts', True),
        'overlapping_shift_alerts': data.get('overlapping_shift_alerts', True),
        'missing_checkout_alerts': data.get('missing_checkout_alerts', True),
        'notification_email': data.get('notification_email', user['email'])
    }
    
    # Log the settings update
    log_admin_action('update_notification_settings', 
                     target_user_email=user['email'], 
                     details=f"Updated notification settings: {updated_settings}",
                     user_email=user.get('email', 'system'))
    
    return jsonify({'message': 'Notification settings updated successfully', 'settings': updated_settings})







if __name__ == '__main__':
    # Ensure data directories exist
    data_dirs = [
        'data/core',
        'logs',
        'static/snapshots'
    ]
    for dir_path in data_dirs:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
    
    app.run(debug=True, host='0.0.0.0', port=5000)

