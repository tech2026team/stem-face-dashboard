"""Admin blueprint"""
from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for, session
from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for, session

admin_bp = Blueprint('admin', __name__)

from app.auth.service import get_current_user, get_user_role as service_get_user_role

def get_user_role(user=None):
    """Get user's role"""
    if not user:
        user = get_current_user()
    if not user:
        return None
    
    # Try different ways to get role
    role = user.get('role')
    if not role:
        role = user.get('user_metadata', {}).get('role')
    if not role and 'email' in user:
        # Try to get from auth module
        try:
            role = service_get_user_role(user.get('email'))
        except:
            pass
    return role

def require_admin_access():
    """Check if user is authenticated and has admin access
    
    Access is granted if:
    1. User has admin/system_admin/manager role, OR
    2. User email/user_id is in the allowed feature toggle users list
    """
    import os
    user = get_current_user()
    if not user:
        # Try to get login route name
        try:
            return None, redirect('/login')
        except:
            return None, redirect(url_for('login'))
    
    # Get user identifier (email or user_id)
    user_email = user.get('email', '')
    user_id = user.get('id') or user.get('user_id', '')
    
    # Check if user has admin role
    user_role = get_user_role(user)
    allowed_roles = ['admin', 'system_admin', 'manager']
    
    if user_role not in allowed_roles:
        return None, None  # Will return 403
    
    return user, None

@admin_bp.route('/dashboard')
def dashboard():
    """Admin dashboard with feature toggles - for authorized users only"""
    user, redirect_response = require_admin_access()
    if redirect_response:
        flash('Please login to access the admin dashboard', 'warning')
        return redirect_response
    
    if not user:
        flash('Access denied. Admin privileges required.', 'danger')
        return redirect('/login'), 403
    
    try:
        # Get user info for display
        user_email = user.get('email', 'Unknown')
        user_name = user.get('full_name') or user.get('user_metadata', {}).get('full_name', user_email)
        user_role = get_user_role(user) or 'Unknown'
        
        return render_template('admin/dashboard.html', 
                             current_user={
                                 'email': user_email,
                                 'name': user_name,
                                 'role': user_role
                             })
    except Exception as e:
        flash(f'Error loading dashboard: {str(e)}', 'danger')
        return render_template('admin/dashboard.html', 
                             current_user={'email': 'Unknown', 'name': 'Unknown', 'role': 'Unknown'})



@admin_bp.route('/api/current-user')
def api_current_user():
    """Get current user info for navbar display"""
    user = get_current_user()
    if not user:
        return jsonify({'error': 'Not authenticated'}), 401
    
    user_email = user.get('email', 'Unknown')
    user_name = user.get('full_name') or user.get('user_metadata', {}).get('full_name', user_email.split('@')[0])
    user_role = get_user_role(user) or 'user'
    
    return jsonify({
        'email': user_email,
        'name': user_name,
        'role': user_role
    })

@admin_bp.route('/api/dashboard-stats')
def api_dashboard_stats():
    """Get dashboard statistics - Updated to use Phase 1 & 2 SchedulingAnalytics"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        user, redirect_response = require_admin_access()
        if redirect_response:
            logger.warning(f"Dashboard stats unauthorized: redirecting. User: {user}")
            return jsonify({'error': 'Unauthorized', 'redirect': True}), 401
        if not user:
            logger.warning("Dashboard stats unauthorized: no user")
            return jsonify({'error': 'Unauthorized'}), 401
            
        logger.info(f"Fetching dashboard stats for user: {user.get('email')}")
    except Exception as e:
        logger.error(f"Auth check error: {e}")
        return jsonify({'error': 'Authentication error'}), 500
    
    try:
        from app.core.analytics import SchedulingAnalytics
        from app.core.scheduling_manager import SchedulingManager
        from datetime import datetime
        import pandas as pd
        import os
        
        # Use SchedulingAnalytics for appointment-based stats (Phase 1 & 2)
        analytics = SchedulingAnalytics(data_dir='data/core')
        manager = SchedulingManager(data_dir='data/core')
        
        # Get today's date for filtering
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Count tutors (from SchedulingManager)
        total_tutors = len(manager.tutors) if not manager.tutors.empty else 0
        
        # Count active courses (from SchedulingManager)
        courses_df = manager.courses if hasattr(manager, 'courses') else pd.DataFrame()
        if not courses_df.empty and 'active' in courses_df.columns:
            active_courses = len(courses_df[courses_df['active'] == True])
        else:
            active_courses = len(courses_df) if not courses_df.empty else 0
        
        # Count today's appointments (using Phase 1 & 2 data)
        appointments_df = analytics.appointments
        appointments_today = 0
        if not appointments_df.empty and 'appointment_date' in appointments_df.columns:
            # Convert appointment_date to string for comparison
            appointments_df['date_str'] = pd.to_datetime(appointments_df['appointment_date']).dt.strftime('%Y-%m-%d')
            appointments_today = len(appointments_df[appointments_df['date_str'] == today])
        
        # Count total users
        users_file = 'data/core/users.csv'
        total_users = 0
        if os.path.exists(users_file):
            try:
                users_df = pd.read_csv(users_file)
                total_users = len(users_df)
            except:
                pass
        
        # Count available slots (Phase 1 & 2 - using availability.csv with slot_status)
        available_slots = 0
        availability_file = 'data/core/availability.csv'
        if os.path.exists(availability_file):
            try:
                availability_df = pd.read_csv(availability_file)
                if not availability_df.empty:
                    # Count slots with status 'available' (Phase 1 field)
                    if 'slot_status' in availability_df.columns:
                        available_slots = len(availability_df[availability_df['slot_status'] == 'available'])
                    else:
                        # Fallback: count all if slot_status doesn't exist
                        available_slots = len(availability_df)
            except Exception as e:
                logger.warning(f"Error counting availability slots: {e}")
        
        # Phase 1 & 2 Enhanced Stats (optional - can be added to dashboard later)
        summary = analytics.get_summary_stats()
        pending_confirmations = summary.get('pending_confirmations', 0)
        student_booked = summary.get('student_booked_count', 0)
        admin_scheduled = summary.get('admin_scheduled_count', 0)
        
        stats = {
            'total_tutors': total_tutors,
            'active_courses': active_courses,
            'appointments_today': appointments_today,
            'total_users': total_users,
            'available_slots': available_slots,  # New KPI card
            # Phase 1 & 2 enhanced metrics (for future use)
            'pending_confirmations': pending_confirmations,
            'student_booked': student_booked,
            'admin_scheduled': admin_scheduled,
            'total_appointments': summary.get('total_checkins', 0)
        }
        
        # Sanitize stats to remove NaN values
        def sanitize_for_json(obj):
            import math
            import numpy as np
            if isinstance(obj, dict):
                return {k: sanitize_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [sanitize_for_json(v) for v in obj]
            elif isinstance(obj, float) and (math.isnan(obj) or np.isnan(obj)):
                return None
            elif pd.isna(obj):
                return None
            return obj
            
        return jsonify(sanitize_for_json(stats))
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error getting dashboard stats: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
