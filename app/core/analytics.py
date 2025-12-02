"""
Core Analytics Module for Scheduling System
Analyzes appointments, shifts, and tutor scheduling data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchedulingAnalytics:
    """Analytics for scheduling system - appointments, shifts, availability"""
    
    def __init__(self, data_dir='data/core'):
        self.data_dir = Path(data_dir)
        self.appointments = self.load_appointments()
        self.tutors = self.load_tutors()
        self.users = self.load_users()
        self.courses = self.load_courses()
        self.shifts = self.load_shifts()
        self.shift_assignments = self.load_shift_assignments()
        self.availability = self.load_availability()
        
        # Merge tutor data with user data for full names
        if not self.tutors.empty and not self.users.empty:
            self.tutors = self.tutors.merge(
                self.users[['user_id', 'full_name', 'email']], 
                on='user_id', 
                how='left'
            )
            # Fill NaN full_names
            if 'full_name' in self.tutors.columns:
                self.tutors['full_name'] = self.tutors['full_name'].fillna('')

    
    def load_appointments(self):
        """Load appointments data"""
        try:
            df = pd.read_csv(self.data_dir / 'appointments.csv')
            if not df.empty:
                df['appointment_date'] = pd.to_datetime(df['appointment_date'])
                df['start_time'] = pd.to_datetime(df['start_time'], format='%H:%M:%S').dt.time
                df['end_time'] = pd.to_datetime(df['end_time'], format='%H:%M:%S').dt.time
                # Calculate duration in hours
                df['duration_hours'] = df.apply(
                    lambda row: (datetime.combine(datetime.today(), row['end_time']) - 
                                datetime.combine(datetime.today(), row['start_time'])).total_seconds() / 3600,
                    axis=1
                )
            return df
        except Exception as e:
            logger.error(f"Error loading appointments: {e}")
            return pd.DataFrame()
    
    def load_tutors(self):
        """Load tutors data"""
        try:
            return pd.read_csv(self.data_dir / 'tutors.csv')
        except Exception as e:
            logger.error(f"Error loading tutors: {e}")
            return pd.DataFrame()
    
    def load_users(self):
        """Load users data"""
        try:
            return pd.read_csv(self.data_dir / 'users.csv')
        except Exception as e:
            logger.error(f"Error loading users: {e}")
            return pd.DataFrame()
    
    def load_courses(self):
        """Load courses data"""
        try:
            return pd.read_csv(self.data_dir / 'courses.csv')
        except Exception as e:
            logger.error(f"Error loading courses: {e}")
            return pd.DataFrame()
    
    def load_shifts(self):
        """Load shifts data"""
        try:
            return pd.read_csv(self.data_dir / 'shifts.csv')
        except Exception as e:
            logger.error(f"Error loading shifts: {e}")
            return pd.DataFrame()
    
    def load_shift_assignments(self):
        """Load shift assignments data"""
        try:
            df = pd.read_csv(self.data_dir / 'shift_assignments.csv')
            if not df.empty:
                df['start_date'] = pd.to_datetime(df['start_date'])
                df['end_date'] = pd.to_datetime(df['end_date'])
            return df
        except Exception as e:
            logger.error(f"Error loading shift assignments: {e}")
            return pd.DataFrame()
    
    def load_availability(self):
        """Load availability data"""
        try:
            df = pd.read_csv(self.data_dir / 'availability.csv')
            if not df.empty:
                df['effective_date'] = pd.to_datetime(df['effective_date'])
                df['end_date'] = pd.to_datetime(df['end_date'])
            return df
        except Exception as e:
            logger.error(f"Error loading availability: {e}")
            return pd.DataFrame()
    
    def filter_data(self, start_date=None, end_date=None, tutor_ids=None, course_ids=None, status=None, 
                    duration=None, day_type=None, shift_start_hour=None, shift_end_hour=None, **kwargs):
        """Filter appointments based on criteria"""
        df = self.appointments.copy()
        
        if df.empty:
            return df
        
        if start_date:
            df = df[df['appointment_date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['appointment_date'] <= pd.to_datetime(end_date)]
        if tutor_ids:
            if isinstance(tutor_ids, str):
                tutor_ids = [tid.strip() for tid in tutor_ids.split(',') if tid.strip()]
            if tutor_ids:
                df = df[df['tutor_id'].isin(tutor_ids)]
        if course_ids:
            if isinstance(course_ids, str):
                course_ids = [cid.strip() for cid in course_ids.split(',') if cid.strip()]
            if course_ids:
                df = df[df['course_id'].isin(course_ids)]
        if status:
            df = df[df['status'] == status]
        
        # Filter by duration if provided
        if duration:
            if isinstance(duration, str):
                # Parse duration range (e.g., "1-2" for 1-2 hours)
                if '-' in duration:
                    min_dur, max_dur = map(float, duration.split('-'))
                    df = df[(df['duration_hours'] >= min_dur) & (df['duration_hours'] <= max_dur)]
                else:
                    # Exact duration
                    target_dur = float(duration)
                    df = df[df['duration_hours'] == target_dur]
        
        # Filter by day type (weekday/weekend)
        if day_type:
            if day_type.lower() == 'weekday':
                df = df[df['appointment_date'].dt.dayofweek < 5]
            elif day_type.lower() == 'weekend':
                df = df[df['appointment_date'].dt.dayofweek >= 5]
        
        # Filter by shift hours if provided
        if shift_start_hour is not None or shift_end_hour is not None:
            if 'start_time' in df.columns:
                # Convert time to hour (handle both time objects and strings)
                def get_hour(time_val):
                    if pd.isna(time_val):
                        return None
                    if isinstance(time_val, str):
                        try:
                            return pd.to_datetime(time_val, format='%H:%M:%S').hour
                        except:
                            return None
                    elif hasattr(time_val, 'hour'):
                        return time_val.hour
                    return None
                
                df['_hour'] = df['start_time'].apply(get_hour)
                if shift_start_hour is not None:
                    df = df[df['_hour'] >= int(shift_start_hour)]
                if shift_end_hour is not None:
                    df = df[df['_hour'] <= int(shift_end_hour)]
                df = df.drop(columns=['_hour'], errors='ignore')
        
        return df
    
    def get_chart_data(self, dataset, **filters):
        """Get chart data for specified dataset"""
        methods = {
            'appointments_per_tutor': self.appointments_per_tutor,
            'hours_per_tutor': self.hours_per_tutor,
            'daily_appointments': self.daily_appointments,
            'daily_hours': self.daily_hours,
            'appointments_by_status': self.appointments_by_status,
            'appointments_by_course': self.appointments_by_course,
            'appointments_per_day_of_week': self.appointments_per_day_of_week,
            'hourly_appointments_dist': self.hourly_appointments_distribution,
            'hourly_distribution': self.hourly_appointments_distribution,
            'avg_appointment_duration': self.avg_appointment_duration_per_tutor,
            'tutor_workload': self.tutor_workload,
            'course_popularity': self.course_popularity,
            'monthly_appointments': self.monthly_appointments,
            'tutor_availability_hours': self.tutor_availability_hours,
            'shift_coverage': self.shift_coverage,
            'appointment_trends': self.appointment_trends,
            'cumulative_appointments': self.cumulative_appointments,
            'cumulative_hours': self.cumulative_hours,
            'monthly_hours': self.monthly_hours,
            'avg_hours_per_day_of_week': self.avg_hours_per_day_of_week,
            'appointment_duration_distribution': self.appointment_duration_distribution,
        }
        
        method = methods.get(dataset)
        if method:
            return method(**filters)
        else:
            logger.warning(f"Unknown dataset: {dataset}")
            return {}
    
    def appointments_per_tutor(self, **filters):
        """Get number of appointments per tutor"""
        df = self.filter_data(**filters)
        
        # Initialize with 0 for all tutors
        result = {str(tid): 0 for tid in self.tutors['tutor_id'].unique()}
        
        if not df.empty:
            # Group by tutor and count
            counts = df.groupby('tutor_id').size().to_dict()
            result.update({str(k): v for k, v in counts.items()})
        
        # Replace tutor IDs with names
        tutor_names = {}
        for tid, count in result.items():
            tutor = self.tutors[self.tutors['tutor_id'].astype(str) == str(tid)]
            if not tutor.empty:
                name = tutor.iloc[0].get('full_name', f"Tutor {tid}")
                # If name is empty string, fall back to ID
                if not name: name = f"Tutor {tid}"
                tutor_names[name] = count
            else:
                tutor_names[f"Tutor {tid}"] = count
        
        return tutor_names
    
    def hours_per_tutor(self, **filters):
        """Get total scheduled hours per tutor"""
        df = self.filter_data(**filters)
        
        # Initialize with 0 for all tutors
        result = {str(tid): 0.0 for tid in self.tutors['tutor_id'].unique()}
        
        if not df.empty:
            # Sum duration hours by tutor
            hours_data = df.groupby('tutor_id')['duration_hours'].sum().to_dict()
            result.update({str(k): float(v) for k, v in hours_data.items()})
        
        # Replace tutor IDs with names
        tutor_hours = {}
        for tid, hours in result.items():
            tutor = self.tutors[self.tutors['tutor_id'].astype(str) == str(tid)]
            if not tutor.empty:
                name = tutor.iloc[0].get('full_name', f"Tutor {tid}")
                if not name: name = f"Tutor {tid}"
                tutor_hours[name] = round(float(hours), 2)
            else:
                tutor_hours[f"Tutor {tid}"] = round(float(hours), 2)
        
        return tutor_hours
    
    def daily_appointments(self, **filters):
        """Get appointments per day"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        result = df.groupby(df['appointment_date'].dt.date).size().to_dict()
        return {str(k): int(v) for k, v in result.items()}
    
    def daily_hours(self, **filters):
        """Get scheduled hours per day"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        result = df.groupby(df['appointment_date'].dt.date)['duration_hours'].sum().to_dict()
        return {str(k): round(float(v), 2) for k, v in result.items()}
    
    def appointments_by_status(self, **filters):
        """Get appointments grouped by status"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        result = df['status'].value_counts().to_dict()
        return {str(k).title(): int(v) for k, v in result.items()}
    
    def appointments_by_course(self, **filters):
        """Get appointments per course"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        # Merge with courses to get course names
        if not self.courses.empty:
            df = df.merge(self.courses[['course_id', 'course_name']], on='course_id', how='left')
            result = df['course_name'].value_counts().to_dict()
        else:
            result = df['course_id'].value_counts().to_dict()
        
        return {str(k): int(v) for k, v in result.items()}
    
    def appointments_per_day_of_week(self, **filters):
        """Get appointments per day of week"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        df['day_of_week'] = df['appointment_date'].dt.day_name()
        result = df['day_of_week'].value_counts().to_dict()
        
        # Order by weekday
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        ordered = {day: result.get(day, 0) for day in days_order}
        return ordered
    
    def hourly_appointments_distribution(self, **filters):
        """Get appointments by hour of day"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        # Extract hour from start_time
        df['hour'] = df['start_time'].apply(lambda x: x.hour if pd.notna(x) else 0)
        result = df['hour'].value_counts().to_dict()
        
        # Fill in all hours 0-23
        hourly = {f"{h:02d}:00": result.get(h, 0) for h in range(24)}
        return hourly
    
    def avg_appointment_duration_per_tutor(self, **filters):
        """Get average appointment duration per tutor"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        result = df.groupby('tutor_id')['duration_hours'].mean().to_dict()
        
        # Replace tutor IDs with names
        tutor_avg = {}
        for tid, avg_hours in result.items():
            tutor = self.tutors[self.tutors['tutor_id'] == tid]
            if not tutor.empty:
                name = tutor.iloc[0].get('full_name', f"Tutor {tid}")
                tutor_avg[name] = round(float(avg_hours), 2)
        
        return tutor_avg if tutor_avg else result
    
    def tutor_workload(self, **filters):
        """Calculate tutor workload (appointments + hours)"""
        df = self.filter_data(**filters)
        
        # Initialize with 0 for all tutors
        result = {}
        all_tutors = self.tutors['tutor_id'].unique()
        
        # Calculate workload if data exists
        workload = {}
        if not df.empty:
            workload = df.groupby('tutor_id')['duration_hours'].sum().to_dict()
            
        for tid in all_tutors:
            tid_str = str(tid)
            tutor = self.tutors[self.tutors['tutor_id'].astype(str) == tid_str]
            if not tutor.empty:
                name = tutor.iloc[0].get('full_name', f"Tutor {tid}")
                if not name: name = f"Tutor {tid}"
                
                # Workload score = total hours (or 0 if not in workload)
                hours = workload.get(tid, 0)
                # Try string key if int failed
                if hours == 0 and tid_str in workload:
                    hours = workload[tid_str]
                    
                result[name] = round(float(hours), 2)
        
        return result
    
    def course_popularity(self, **filters):
        """Get course popularity by appointment count"""
        return self.appointments_by_course(**filters)
    
    def monthly_appointments(self, **filters):
        """Get appointments per month"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        df['month'] = df['appointment_date'].dt.to_period('M')
        result = df.groupby('month').size().to_dict()
        return {str(k): int(v) for k, v in result.items()}
    
    def tutor_availability_hours(self, **filters):
        """Calculate total available hours per tutor"""
        if self.availability.empty:
            return {}
        
        tutor_ids = filters.get('tutor_ids')
        df = self.availability.copy()
        
        if tutor_ids:
            if isinstance(tutor_ids, str):
                tutor_ids = [tid.strip() for tid in tutor_ids.split(',')]
            df = df[df['tutor_id'].isin(tutor_ids)]
        
        # Calculate hours per availability window
        df['hours'] = df.apply(
            lambda row: (datetime.strptime(str(row['end_time']), '%H:%M:%S') - 
                        datetime.strptime(str(row['start_time']), '%H:%M:%S')).total_seconds() / 3600,
            axis=1
        )
        
        result = df.groupby('tutor_id')['hours'].sum().to_dict()
        
        # Replace with names
        tutor_hours = {}
        for tid, hours in result.items():
            tutor = self.tutors[self.tutors['tutor_id'] == tid]
            if not tutor.empty:
                name = tutor.iloc[0].get('full_name', f"Tutor {tid}")
                tutor_hours[name] = round(float(hours), 2)
        
        return tutor_hours
    
    def shift_coverage(self, **filters):
        """Calculate how many shifts are covered by tutors"""
        if self.shift_assignments.empty:
            return {}
        
        # Count assignments per shift
        result = self.shift_assignments.groupby('shift_id').size().to_dict()
        
        # Get shift names
        shift_coverage = {}
        for sid, count in result.items():
            shift = self.shifts[self.shifts['shift_id'] == sid]
            if not shift.empty:
                name = shift.iloc[0].get('shift_name', f"Shift {sid}")
                shift_coverage[name] = int(count)
        
        return shift_coverage
    
    def appointment_trends(self, **filters):
        """Get appointment trends over time"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        # Group by week
        df['week'] = df['appointment_date'].dt.to_period('W')
        result = df.groupby('week').size().to_dict()
        return {str(k): int(v) for k, v in result.items()}
    
    def cumulative_appointments(self, **filters):
        """Get cumulative appointments over time"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        df = df.sort_values('appointment_date')
        df['cumulative'] = range(1, len(df) + 1)
        result = df.groupby(df['appointment_date'].dt.date)['cumulative'].last().to_dict()
        return {str(k): int(v) for k, v in result.items()}
    
    def cumulative_hours(self, **filters):
        """Get cumulative hours over time"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        df = df.sort_values('appointment_date')
        df['cumulative_hours'] = df['duration_hours'].cumsum()
        result = df.groupby(df['appointment_date'].dt.date)['cumulative_hours'].last().to_dict()
        return {str(k): round(float(v), 2) for k, v in result.items()}
    
    def monthly_hours(self, **filters):
        """Get hours per month"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        df['month'] = df['appointment_date'].dt.to_period('M')
        result = df.groupby('month')['duration_hours'].sum().to_dict()
        return {str(k): round(float(v), 2) for k, v in result.items()}
    
    def avg_hours_per_day_of_week(self, **filters):
        """Get average hours per day of week"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        df['day_of_week'] = df['appointment_date'].dt.day_name()
        result = df.groupby('day_of_week')['duration_hours'].mean().to_dict()
        
        # Order by weekday
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        ordered = {day: round(float(result.get(day, 0)), 2) for day in days_order}
        return ordered
    
    def appointment_duration_distribution(self, **filters):
        """Get distribution of appointment durations"""
        df = self.filter_data(**filters)
        if df.empty:
            return {}
        
        # Create duration brackets
        bins = [0, 1, 2, 3, 4, float('inf')]
        labels = ['<1 hour', '1-2 hours', '2-3 hours', '3-4 hours', '4+ hours']
        df['duration_bracket'] = pd.cut(df['duration_hours'], bins=bins, labels=labels, right=False)
        
        result = df['duration_bracket'].value_counts().to_dict()
        return {str(k): int(v) for k, v in result.items()}
    
    def get_summary_stats(self, **filters):
        """
        Get summary statistics for dashboard KPIs
        Returns both new format (appointment-based) and legacy format (check-in based) for compatibility
        """
        df = self.filter_data(**filters)
        
        if df.empty:
            return {
                # New format (appointment-based)
                'total_appointments': 0,
                'total_hours': 0,
                'active_tutors': 0,
                'active_courses': 0,
                'avg_duration': 0,
                # Legacy format (for dashboard compatibility)
                'total_checkins': 0,
                'total_tutors': 0,
                'active_tutors': 0,
                'avg_session_duration': '—',
                'total_hours': '0',
                'avg_daily_hours': '—',
                'peak_checkin_hour': '—',
                'top_day': '—',
                'top_tutor_current_month': '—',
                # Phase 1 enhanced metrics
                'pending_confirmations': 0,
                'student_booked_count': 0,
                'admin_scheduled_count': 0,
                'cancelled_count': 0
            }
        
        # Calculate basic stats
        total_appointments = int(len(df))
        total_hours = round(float(df['duration_hours'].sum()), 2)
        active_tutors = int(df['tutor_id'].nunique())
        active_courses = int(df['course_id'].nunique())
        avg_duration = round(float(df['duration_hours'].mean()), 2)
        
        # Calculate daily average hours
        if not df.empty:
            df['date'] = pd.to_datetime(df['appointment_date']).dt.date
            daily_hours = df.groupby('date')['duration_hours'].sum()
            avg_daily_hours = round(float(daily_hours.mean()), 2) if not daily_hours.empty else 0
        else:
            avg_daily_hours = 0
        
        # Calculate peak hour
        if 'start_time' in df.columns and not df.empty:
            df['hour'] = df['start_time'].apply(lambda x: x.hour if pd.notna(x) and hasattr(x, 'hour') else 0)
            hour_counts = df['hour'].value_counts()
            peak_hour = f"{hour_counts.idxmax():02d}:00" if not hour_counts.empty else '—'
        else:
            peak_hour = '—'
        
        # Calculate most active day
        if not df.empty:
            df['day_name'] = pd.to_datetime(df['appointment_date']).dt.day_name()
            day_counts = df['day_name'].value_counts()
            top_day = day_counts.idxmax()[:3] if not day_counts.empty else '—'  # First 3 letters
        else:
            top_day = '—'
        
        # Calculate top tutor this month
        if not df.empty:
            current_month = pd.Timestamp.now().to_period('M')
            df['month'] = pd.to_datetime(df['appointment_date']).dt.to_period('M')
            month_df = df[df['month'] == current_month]
            if not month_df.empty:
                tutor_hours = month_df.groupby('tutor_id')['duration_hours'].sum()
                top_tutor_id = tutor_hours.idxmax() if not tutor_hours.empty else None
                if top_tutor_id:
                    top_tutor = self.tutors[self.tutors['tutor_id'] == top_tutor_id]
                    top_tutor_name = top_tutor.iloc[0].get('full_name', top_tutor_id) if not top_tutor.empty else top_tutor_id
                else:
                    top_tutor_name = '—'
            else:
                top_tutor_name = '—'
        else:
            top_tutor_name = '—'
        
        # Phase 1 enhanced metrics
        pending_confirmations = 0
        student_booked_count = 0
        admin_scheduled_count = 0
        cancelled_count = 0
        
        if 'confirmation_status' in df.columns:
            pending_confirmations = int(len(df[df['confirmation_status'] == 'pending']))
        
        if 'booking_type' in df.columns:
            student_booked_count = int(len(df[df['booking_type'] == 'student_booked']))
            admin_scheduled_count = int(len(df[df['booking_type'] == 'admin_scheduled']))
        
        if 'status' in df.columns:
            cancelled_count = int(len(df[df['status'] == 'cancelled']))
        
        return {
            # New format (appointment-based)
            'total_appointments': total_appointments,
            'total_hours': total_hours,
            'active_tutors': active_tutors,
            'active_courses': active_courses,
            'avg_duration': avg_duration,
            # Legacy format (for dashboard KPI compatibility)
            'total_checkins': total_appointments,  # Map appointments to check-ins
            'total_tutors': active_tutors,
            'active_tutors': active_tutors,
            'avg_session_duration': f"{avg_duration:.1f}" if avg_duration > 0 else '—',
            'total_hours': str(total_hours),
            'avg_daily_hours': f"{avg_daily_hours:.1f}" if avg_daily_hours > 0 else '—',
            'peak_checkin_hour': peak_hour,
            'top_day': top_day,
            'top_tutor_current_month': top_tutor_name,
            # Phase 1 enhanced metrics
            'pending_confirmations': pending_confirmations,
            'student_booked_count': student_booked_count,
            'admin_scheduled_count': admin_scheduled_count,
            'cancelled_count': cancelled_count
        }
    
    def _convert_numpy_types(self, obj):
        """Convert numpy types to native Python types for JSON serialization"""
        if isinstance(obj, dict):
            return {self._convert_numpy_types(key): self._convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._convert_numpy_types(item) for item in obj]
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif pd.isna(obj):
            return None
        else:
            return obj

