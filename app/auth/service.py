"""
Authentication module for Tutor Face Recognition Dashboard
Handles Supabase authentication and role-based access control
"""

import os
import logging
import hashlib
import secrets
from functools import wraps
from flask import session, request, jsonify, redirect, url_for, flash
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
from app.auth.utils import USERS_FILE, hash_password as legacy_hash_password
import pandas as pd

# Unified API error helper
def error_response(message, status_code=400, code=None, details=None):
    """Return a consistent JSON error payload.

    message: Human-readable error message
    status_code: HTTP status code to return
    code: Optional short machine code (e.g., AUTH_REQUIRED)
    details: Optional dict with extra context
    """
    payload = {"error": {"message": message}}
    if code:
        payload["error"]["code"] = code
    if details is not None:
        payload["error"]["details"] = details
    return jsonify(payload), status_code

# Bridge to analytics audit logger
try:
    from app.core.audit_logger import log_admin_action as _log_admin_action
except Exception:
    _log_admin_action = None

def log_admin_action(action, target_user_email=None, details="", user_email=None):
    """Proxy to audit_logger.log_admin_action; fail-safe if audit logger not available."""
    try:
        if _log_admin_action:
            _log_admin_action(action, target_user_email=target_user_email, details=details, user_email=user_email)
    except Exception as e:
        logger.error(f"Failed to log admin action '{action}': {e}")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Check if Supabase environment variables are set
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

# Initialize Supabase client only if environment variables are available
supabase = None
if supabase_url and supabase_key:
    try:
        supabase: Client = create_client(supabase_url, supabase_key)
        logger.info("Supabase client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")
        supabase = None
else:
    logger.warning("Supabase environment variables not set. Running in demo mode with local authentication.")

# Role normalization and hierarchy
def normalize_role(role: str) -> str:
    if not role:
        return 'tutor'
    return str(role).strip().lower().replace(' ', '_')

ROLE_HIERARCHY = {
    'tutor': 1,
    'lead_tutor': 2, 
    'manager': 3,
    'admin': 4
}

# Audit log file
AUDIT_LOG_FILE = 'data/legacy/audit_log.csv'

DEMO_USERS = {}

def _resolve_tutor_id_from_logs_by_name(full_name: str):
    """Resolve a numeric tutor_id by matching full_name in face_log_with_expected.csv.
    Returns the most frequent tutor_id as a string, or None.
    """
    try:
        logs_path = 'logs/face_log_with_expected.csv'
        if not os.path.exists(logs_path):
            return None
        df_logs = pd.read_csv(logs_path)
        if 'tutor_name' not in df_logs.columns or 'tutor_id' not in df_logs.columns:
            return None
        mask = df_logs['tutor_name'].astype(str).str.strip().str.lower() == (full_name or '').strip().lower()
        subset = df_logs[mask]
        if subset.empty:
            return None
        mode_series = subset['tutor_id'].astype(str).mode()
        if mode_series.empty:
            return None
        return mode_series.iloc[0]
    except Exception as e:
        logger.warning(f"Could not resolve tutor_id from logs: {e}")
        return None

def get_current_user():
    """Get current authenticated user from session"""
    if 'user' in session:
        return session['user']
    return None

def get_user_role(email=None):
    """Get user's role - current user if no email provided, or specific user by email"""
    if not supabase:
        # Demo mode - use local users
        if email:
            if email in DEMO_USERS:
                return normalize_role(DEMO_USERS[email]['user_metadata'].get('role', 'tutor'))
        else:
            user = get_current_user()
            if user and 'user_metadata' in user:
                return normalize_role(user['user_metadata'].get('role', 'tutor'))
        return 'tutor'
    
    # Supabase mode
    if email:
        # Get role for specific user
        try:
            response = supabase.table('users').select('role').eq('email', email).execute()
            if response.data:
                return normalize_role(response.data[0].get('role', 'tutor'))
        except Exception as e:
            logger.error(f"Error getting user role for {email}: {e}")
            return 'tutor'
    else:
        # Get current user's role
        user = get_current_user()
        if user and 'user_metadata' in user:
            return normalize_role(user['user_metadata'].get('role', 'tutor'))
    return 'tutor'

def get_user_tutor_id():
    """Get current user's tutor_id for data filtering"""
    user = get_current_user()
    if user and 'user_metadata' in user:
        tid = user['user_metadata'].get('tutor_id')
        # If missing or looks like a CSV user_id (starts with 'U'), try resolving from logs by name
        if not tid or (isinstance(tid, str) and tid.upper().startswith('U')):
            full_name = user['user_metadata'].get('full_name') or user.get('full_name')
            resolved = _resolve_tutor_id_from_logs_by_name(full_name) if full_name else None
            if resolved:
                user['user_metadata']['tutor_id'] = str(resolved)
                session['user'] = user
                return str(resolved)
        return tid
    return None

def has_role_access(required_role):
    """Check if current user has required role access"""
    current_role = get_user_role()
    if not current_role:
        return False
    
    current_level = ROLE_HIERARCHY.get(normalize_role(current_role), 0)
    required_level = ROLE_HIERARCHY.get(normalize_role(required_role), 999)
    
    return current_level >= required_level

def login_required(f):
    """Decorator to require authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not get_current_user():
            if request.is_json:
                return error_response("Authentication required", status_code=401, code="AUTH_REQUIRED")
            return redirect(url_for('auth.login')) # Updated to auth.login
        return f(*args, **kwargs)
    return decorated_function

def role_required(required_role):
    """Decorator to require specific role"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not get_current_user():
                if request.is_json:
                    return error_response("Authentication required", status_code=401, code="AUTH_REQUIRED")
                return redirect(url_for('auth.login')) # Updated to auth.login
            
            if not has_role_access(required_role):
                if request.is_json:
                    return error_response("Insufficient permissions", status_code=403, code="FORBIDDEN", details={"required_role": required_role})
                flash('You do not have permission to access this page.', 'error')
                return redirect(url_for('index'))
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def hash_password(password, salt=None):
    """Hash password with salt for secure storage"""
    if salt is None:
        salt = secrets.token_hex(16)
    password_hash = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), 100000)
    return salt, password_hash.hex()

def verify_password(password, salt, stored_hash):
    """Verify password against stored hash"""
    try:
        _, computed_hash = hash_password(password, salt)
        return secrets.compare_digest(computed_hash, stored_hash)
    except Exception as e:
        logger.error(f"Password verification error: {e}")
        return False

def authenticate_user(email, password):
    """
    Hybrid authentication - tries Supabase Auth first, then custom users table, then demo users, then local CSV users
    """
    if not email or not password:
        return False, "Email and password are required."
    
    # Helper: local CSV fallback auth
    def _try_csv_auth():
        import os
        if os.path.exists(USERS_FILE):
            try:
                df_local = pd.read_csv(USERS_FILE)
            except Exception as csv_error:
                logger.error(f"Failed to read USERS_FILE {USERS_FILE}: {csv_error}")
                return False, "Authentication temporarily unavailable. Please try again later."
            user_row = df_local[df_local['email'] == email]
            if not user_row.empty:
                user = user_row.iloc[0]
                if not user.get('active', user.get('is_active', True)):
                    return False, "User account is inactive."
                stored_hash = user.get('password_hash', '')
                if stored_hash:
                    try:
                        if isinstance(stored_hash, str) and len(stored_hash.strip()) == 32:
                            candidate = hashlib.md5(password.encode()).hexdigest()
                        else:
                            candidate = legacy_hash_password(password)
                    except Exception:
                        candidate = legacy_hash_password(password)
                    if candidate == stored_hash:
                        session['user'] = {
                            'id': user.get('user_id'),
                            'email': user.get('email'),
                            'user_metadata': {
                                'role': user.get('role', 'tutor'),
                                'full_name': user.get('full_name', ''),
                                'tutor_id': user.get('user_id')
                            }
                        }
                        logger.info(f"User {email} authenticated via local CSV users")
                        return True, "Login successful"
            return False, "Invalid email or password."
        return False, "Invalid email or password."
    
    # First try Supabase Auth if available
    if supabase:
        try:
            response = supabase.auth.sign_in_with_password({
                "email": email,
                "password": password
            })
            if response.user:
                # Store user in session
                session['user'] = {
                    'id': response.user.id,
                    'email': response.user.email,
                    'user_metadata': response.user.user_metadata or {}
                }
                if hasattr(response, 'session') and response.session:
                    session['access_token'] = response.session.access_token
                logger.info(f"User {email} authenticated via Supabase Auth")
                return True, "Login successful"
        except Exception as supabase_error:
            logger.warning(f"Supabase Auth failed for {email}: {supabase_error}")
            
            # If Supabase Auth fails, try custom users table
            try:
                # Query custom users table
                response = supabase.table('users').select('*').eq('email', email).execute()
                
                if response.data and len(response.data) > 0:
                    user_data = response.data[0]
                    
                    # Verify password - handle both salt-based and legacy hash-only systems
                    password_hash = user_data.get('password_hash', '')
                    salt = user_data.get('salt', '')

                    # If salt exists, use salt-based verification
                    if salt:
                        password_valid = verify_password(password, salt, password_hash)
                    else:
                        # Legacy system - try direct hash comparison (for existing users)
                        # This is a simple hash comparison for backward compatibility
                        import hashlib
                        simple_hash = hashlib.sha256(password.encode()).hexdigest()
                        password_valid = secrets.compare_digest(simple_hash, password_hash)

                    if password_valid:
                        # Store user in session (mimicking Supabase Auth format)
                        session['user'] = {
                            'id': user_data['id'],
                            'email': user_data['email'],
                            'user_metadata': {
                                'role': user_data.get('role', 'tutor'),
                                'full_name': user_data.get('full_name', ''),
                                'tutor_id': user_data.get('tutor_id')
                            }
                        }
                        logger.info(f"User {email} authenticated via custom users table")
                        return True, "Login successful"
                    else:
                        logger.warning(f"Invalid password for user {email}")
                        return False, "Invalid email or password."
                else:
                    logger.warning(f"User {email} not found in custom users table")
                    # Fall back to local CSV if available
                    csv_ok, csv_msg = _try_csv_auth()
                    return (csv_ok, csv_msg)
                    
            except Exception as custom_error:
                logger.error(f"Custom authentication error for {email}: {custom_error}")
                # Fall back to local CSV if available
                csv_ok, csv_msg = _try_csv_auth()
                return (csv_ok, csv_msg)
    
    # Local CSV fallback (runs whether or not Supabase is configured)
    csv_ok, csv_msg = _try_csv_auth()
    if csv_ok:
        return True, csv_msg
    return False, csv_msg

def logout_user():
    """Logout current user"""
    try:
        # Sign out from Supabase if available
        if supabase:
            supabase.auth.sign_out()
        
        # Clear session
        session.clear()
        logger.info("User logged out successfully")
        return True, "Logged out successfully"
        
    except Exception as e:
        logger.error(f"Logout error: {e}")
        # Clear session even if Supabase logout fails
        session.clear()
        return True, "Logged out successfully"

def validate_user_input(email, password, role, tutor_id=None, full_name=None):
    """Validate user input for registration and updates"""
    errors = []
    
    # Email validation
    if not email or '@' not in email:
        errors.append("Valid email is required")
    
    # Password validation
    if not password or len(password) < 8:
        errors.append("Password must be at least 8 characters long")
    
    # Role validation
    if role not in ROLE_HIERARCHY:
        errors.append(f"Invalid role. Must be one of: {', '.join(ROLE_HIERARCHY.keys())}")
    
    # Tutor ID validation (optional)
    if tutor_id and not str(tutor_id).isdigit():
        errors.append("Tutor ID must be a number")
    
    return errors

def register_user(email, password, role='tutor', tutor_id=None, full_name=None):
    """Register new user (admin only function)"""
    # Validate input
    validation_errors = validate_user_input(email, password, role, tutor_id, full_name)
    if validation_errors:
        return False, "; ".join(validation_errors)
    
    # If Supabase is not available, return demo mode message
    if not supabase:
        return False, "Registration is unavailable in demo mode. Configure Supabase to enable this."
    
    try:
        # Check if user already exists
        existing_user = supabase.table('users').select('id').eq('email', email).execute()
        if existing_user.data and len(existing_user.data) > 0:
            return False, "A user with this email already exists."
        
        # Hash password with salt
        salt, password_hash = hash_password(password)
        
        # Create user metadata
        user_metadata = {
            'role': role,
            'full_name': full_name or email.split('@')[0]
        }
        
        if tutor_id:
            user_metadata['tutor_id'] = tutor_id
            
        # Create user in custom users table
        user_data = {
            'email': email,
            'password_hash': password_hash,
            'salt': salt,
            'role': role,
            'full_name': full_name or email.split('@')[0],
            'tutor_id': tutor_id,
            'created_at': datetime.now().isoformat()
        }
        
        response = supabase.table('users').insert(user_data).execute()
        
        if response.data:
            # Log admin action
            log_admin_action(
                action="CREATE_USER",
                target_user_email=email,
                details=f"Created user with role: {role}, tutor_id: {tutor_id or 'None'}"
            )
            logger.info(f"User {email} created successfully")
            return True, "User created successfully."
        else:
            logger.error(f"Failed to create user {email}")
            return False, "Failed to create user."
            
    except Exception as e:
        logger.error(f"Registration error for {email}: {e}")
        return False, "Registration failed due to a server error. Please try again later."

def update_user_role(user_id, new_role, tutor_id=None, full_name=None, email=None):
    """Update user role (admin only function) and keep CSV/session in sync."""
    # Update Supabase if available
    if supabase:
        try:
            user_metadata = {'role': new_role}
            if tutor_id:
                user_metadata['tutor_id'] = tutor_id
            if full_name:
                user_metadata['full_name'] = full_name
            response = supabase.auth.admin.update_user_by_id(
                user_id,
                {"user_metadata": user_metadata}
            )
            if not getattr(response, 'user', None):
                return False, "Failed to update user role."
        except Exception as e:
            logger.error(f"Update user role error for {user_id}: {e}")
            return False, "Unable to update user role at this time."

    # Update local CSV users file
    try:
        if os.path.exists(USERS_FILE):
            df = pd.read_csv(USERS_FILE)
            if email:
                mask = df['email'] == email
            else:
                mask = df['user_id'].astype(str) == str(user_id)
            if mask.any():
                df.loc[mask, 'role'] = new_role
                if tutor_id is not None:
                    if 'tutor_id' in df.columns:
                        df.loc[mask, 'tutor_id'] = tutor_id
                if full_name:
                    if 'full_name' in df.columns:
                        df.loc[mask, 'full_name'] = full_name
                df.to_csv(USERS_FILE, index=False)
    except Exception as e:
        logger.warning(f"Failed to update local users CSV for role change: {e}")

    # If the target is the current session user, refresh session metadata
    current = session.get('user')
    if current and (current.get('email') == email or str(current.get('id')) == str(user_id)):
        meta = current.get('user_metadata', {})
        meta['role'] = new_role
        if tutor_id is not None:
            meta['tutor_id'] = tutor_id
        if full_name:
            meta['full_name'] = full_name
        current['user_metadata'] = meta
        session['user'] = current

    # Log admin action
    log_admin_action(
        action="UPDATE_USER_ROLE",
        target_user_email=email or "Unknown",
        details=f"Changed role to {new_role}, tutor_id: {tutor_id or 'None'}"
    )
    return True, "User role updated successfully."

def get_all_users():
    """Get all users (admin only function)"""
    try:
        # Note: This requires admin privileges
        response = supabase.auth.admin.list_users()
        return response.users if response else []
    except Exception as e:
        # Silently handle permission errors to avoid console spam
        if "User not allowed" in str(e) or "permission" in str(e).lower():
            return []  # Return empty list for permission errors
        print(f"Error fetching users: {e}")
        return []

def filter_data_by_role(df, user_role=None, user_tutor_id=None):
    """Filter dataframe based on user role and permissions (legacy function)"""
    # Import the new permission system
    try:
        from permissions import filter_data_by_permissions
        from permission_middleware import get_user_capabilities
        
        # Get current user context
        user = get_current_user()
        user_email = user.get('email') if user else None
        
        # Use the new permission-based filtering
        return filter_data_by_permissions(df, user_role, user_tutor_id, user_email)
    except ImportError:
        # Fallback to original logic if new system not available
        if not user_role:
            user_role = get_user_role()
        if not user_tutor_id:
            user_tutor_id = get_user_tutor_id()
        
        # Managers and admins see all data
        if user_role in ['manager', 'admin']:
            return df
        
        # Lead tutors see all data (can be restricted if needed)
        if user_role == 'lead_tutor':
            return df
        
        # Regular tutors only see their own data
        if user_role == 'tutor':
            # Primary: filter by tutor_id when available
            if user_tutor_id and 'tutor_id' in df.columns:
                scoped = df[df['tutor_id'].astype(str) == str(user_tutor_id)]
                if not scoped.empty:
                    return scoped
            # Fallback: filter by full_name matching
            user = get_current_user()
            full_name = None
            if user and 'user_metadata' in user:
                full_name = user['user_metadata'].get('full_name')
            if full_name and 'tutor_name' in df.columns:
                scoped = df[df['tutor_name'].astype(str).str.strip().str.lower() == full_name.strip().lower()]
                return scoped
        
        # If no matching conditions, return empty dataframe
        return df.iloc[0:0]  # Empty dataframe with same structure
