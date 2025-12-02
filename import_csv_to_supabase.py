"""
Import CSV data from data/core/ into Supabase database
"""
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env file")
    exit(1)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Data directory
DATA_DIR = Path("data/core")

def convert_bool(value):
    """Convert various boolean representations to Python bool"""
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', 't', '1', 'yes', 'y')
    return bool(value)

def convert_datetime(value):
    """Convert datetime string to ISO format"""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        try:
            # Try parsing various formats
            dt = pd.to_datetime(value)
            return dt.isoformat()
        except:
            return value
    return value

def import_users():
    """Import users from users.csv"""
    csv_file = DATA_DIR / "users.csv"
    if not csv_file.exists():
        logger.warning(f"File not found: {csv_file}")
        return 0
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Found {len(df)} users to import")
        
        records = []
        for _, row in df.iterrows():
            record = {
                'user_id': str(row.get('user_id', '')),
                'email': str(row.get('email', '')),
                'password_hash': str(row.get('password_hash', '')) if pd.notna(row.get('password_hash')) else None,
                'full_name': str(row.get('full_name', '')) if pd.notna(row.get('full_name')) else None,
                'role': str(row.get('role', 'tutor')).lower(),
                'active': convert_bool(row.get('active', row.get('is_active', True))),
                'tutor_id': str(row.get('tutor_id', '')) if pd.notna(row.get('tutor_id')) else None,
            }
            
            # Handle timestamps
            if pd.notna(row.get('created_at')):
                record['created_at'] = convert_datetime(row['created_at'])
            if pd.notna(row.get('last_login')):
                record['last_login'] = convert_datetime(row['last_login'])
            
            records.append(record)
        
        # Insert in batches
        batch_size = 100
        total_inserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                response = supabase.table('users').upsert(batch).execute()
                total_inserted += len(batch)
                logger.info(f"Inserted {total_inserted}/{len(records)} users")
            except Exception as e:
                logger.error(f"Error inserting users batch {i//batch_size + 1}: {e}")
                # Try inserting one by one to find problematic records
                for record in batch:
                    try:
                        supabase.table('users').upsert(record).execute()
                        total_inserted += 1
                    except Exception as err:
                        logger.error(f"Failed to insert user {record.get('email')}: {err}")
        
        logger.info(f"[SUCCESS] Imported {total_inserted} users")
        return total_inserted
        
    except Exception as e:
        logger.error(f"Error importing users: {e}")
        return 0

def import_tutors():
    """Import tutors from tutors.csv"""
    csv_file = DATA_DIR / "tutors.csv"
    if not csv_file.exists():
        logger.warning(f"File not found: {csv_file}")
        return 0
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Found {len(df)} tutors to import")
        
        records = []
        for _, row in df.iterrows():
            record = {
                'tutor_id': str(row.get('tutor_id', '')),
                'user_id': str(row.get('user_id', '')) if pd.notna(row.get('user_id')) else None,
                'bio': str(row.get('bio', '')) if pd.notna(row.get('bio')) else None,
                'specializations': str(row.get('specializations', '')) if pd.notna(row.get('specializations')) else None,
                'max_appointments_per_day': int(row.get('max_appointments_per_day', 10)) if pd.notna(row.get('max_appointments_per_day')) else 10,
                'is_available': convert_bool(row.get('is_available', True)),
            }
            
            if pd.notna(row.get('joined_date')):
                try:
                    record['joined_date'] = pd.to_datetime(row['joined_date']).date().isoformat()
                except:
                    pass
            
            records.append(record)
        
        # Insert in batches
        batch_size = 100
        total_inserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                response = supabase.table('tutors').upsert(batch).execute()
                total_inserted += len(batch)
                logger.info(f"Inserted {total_inserted}/{len(records)} tutors")
            except Exception as e:
                logger.error(f"Error inserting tutors batch {i//batch_size + 1}: {e}")
                # Try one by one
                for record in batch:
                    try:
                        supabase.table('tutors').upsert(record).execute()
                        total_inserted += 1
                    except Exception as err:
                        logger.error(f"Failed to insert tutor {record.get('tutor_id')}: {err}")
        
        logger.info(f"[SUCCESS] Imported {total_inserted} tutors")
        return total_inserted
        
    except Exception as e:
        logger.error(f"Error importing tutors: {e}")
        return 0

def import_courses():
    """Import courses from courses.csv"""
    csv_file = DATA_DIR / "courses.csv"
    if not csv_file.exists():
        logger.warning(f"File not found: {csv_file}")
        return 0
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Found {len(df)} courses to import")
        
        records = []
        for _, row in df.iterrows():
            record = {
                'course_id': str(row.get('course_id', '')),
                'course_code': str(row.get('course_code', '')) if pd.notna(row.get('course_code')) else None,
                'course_name': str(row.get('course_name', '')),
                'department': str(row.get('department', '')) if pd.notna(row.get('department')) else None,
                'description': str(row.get('description', '')) if pd.notna(row.get('description')) else None,
                'is_active': convert_bool(row.get('is_active', True)),
            }
            records.append(record)
        
        # Insert in batches
        batch_size = 100
        total_inserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                response = supabase.table('courses').upsert(batch).execute()
                total_inserted += len(batch)
                logger.info(f"Inserted {total_inserted}/{len(records)} courses")
            except Exception as e:
                logger.error(f"Error inserting courses batch {i//batch_size + 1}: {e}")
                for record in batch:
                    try:
                        supabase.table('courses').upsert(record).execute()
                        total_inserted += 1
                    except Exception as err:
                        logger.error(f"Failed to insert course {record.get('course_id')}: {err}")
        
        logger.info(f"[SUCCESS] Imported {total_inserted} courses")
        return total_inserted
        
    except Exception as e:
        logger.error(f"Error importing courses: {e}")
        return 0

def import_appointments():
    """Import appointments from appointments.csv"""
    csv_file = DATA_DIR / "appointments.csv"
    if not csv_file.exists():
        logger.warning(f"File not found: {csv_file}")
        return 0
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Found {len(df)} appointments to import")
        
        records = []
        for _, row in df.iterrows():
            record = {
                'appointment_id': str(row.get('appointment_id', '')),
                'tutor_id': str(row.get('tutor_id', '')) if pd.notna(row.get('tutor_id')) else None,
                'student_name': str(row.get('student_name', '')) if pd.notna(row.get('student_name')) else None,
                'student_email': str(row.get('student_email', '')) if pd.notna(row.get('student_email')) else None,
                'course_id': str(row.get('course_id', '')) if pd.notna(row.get('course_id')) else None,
                'status': str(row.get('status', 'scheduled')).lower(),
                'booking_type': str(row.get('booking_type', 'student_booked')).lower(),
                'confirmation_status': str(row.get('confirmation_status', 'pending')).lower() if pd.notna(row.get('confirmation_status')) else 'pending',
                'notes': str(row.get('notes', '')) if pd.notna(row.get('notes')) else None,
            }
            
            # Handle dates and times
            if pd.notna(row.get('appointment_date')):
                try:
                    record['appointment_date'] = pd.to_datetime(row['appointment_date']).date().isoformat()
                except:
                    pass
            
            if pd.notna(row.get('start_time')):
                try:
                    time_str = str(row['start_time'])
                    if ':' in time_str:
                        record['start_time'] = time_str
                except:
                    pass
            
            if pd.notna(row.get('end_time')):
                try:
                    time_str = str(row['end_time'])
                    if ':' in time_str:
                        record['end_time'] = time_str
                except:
                    pass
            
            records.append(record)
        
        # Insert in batches
        batch_size = 100
        total_inserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                response = supabase.table('appointments').upsert(batch).execute()
                total_inserted += len(batch)
                logger.info(f"Inserted {total_inserted}/{len(records)} appointments")
            except Exception as e:
                logger.error(f"Error inserting appointments batch {i//batch_size + 1}: {e}")
                for record in batch:
                    try:
                        supabase.table('appointments').upsert(record).execute()
                        total_inserted += 1
                    except Exception as err:
                        logger.error(f"Failed to insert appointment {record.get('appointment_id')}: {err}")
        
        logger.info(f"[SUCCESS] Imported {total_inserted} appointments")
        return total_inserted
        
    except Exception as e:
        logger.error(f"Error importing appointments: {e}")
        return 0

def import_shifts():
    """Import shifts from shifts.csv"""
    csv_file = DATA_DIR / "shifts.csv"
    if not csv_file.exists():
        logger.warning(f"File not found: {csv_file}")
        return 0
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Found {len(df)} shifts to import")
        
        records = []
        for _, row in df.iterrows():
            record = {
                'shift_id': str(row.get('shift_id', '')),
                'shift_name': str(row.get('shift_name', '')),
                'start_time': str(row.get('start_time', '')) if pd.notna(row.get('start_time')) else None,
                'end_time': str(row.get('end_time', '')) if pd.notna(row.get('end_time')) else None,
                'days_of_week': str(row.get('days_of_week', '')) if pd.notna(row.get('days_of_week')) else None,
                'created_by': str(row.get('created_by', '')) if pd.notna(row.get('created_by')) else None,
                'active': convert_bool(row.get('active', True)),
            }
            records.append(record)
        
        if records:
            try:
                response = supabase.table('shifts').upsert(records).execute()
                logger.info(f"[SUCCESS] Imported {len(records)} shifts")
                return len(records)
            except Exception as e:
                logger.error(f"Error inserting shifts: {e}")
                return 0
        return 0
        
    except Exception as e:
        logger.error(f"Error importing shifts: {e}")
        return 0

def import_shift_assignments():
    """Import shift assignments from shift_assignments.csv"""
    csv_file = DATA_DIR / "shift_assignments.csv"
    if not csv_file.exists():
        logger.warning(f"File not found: {csv_file}")
        return 0
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Found {len(df)} shift assignments to import")
        
        records = []
        for _, row in df.iterrows():
            record = {
                'assignment_id': str(row.get('assignment_id', '')),
                'shift_id': str(row.get('shift_id', '')) if pd.notna(row.get('shift_id')) else None,
                'tutor_id': str(row.get('tutor_id', '')) if pd.notna(row.get('tutor_id')) else None,
                'tutor_name': str(row.get('tutor_name', '')) if pd.notna(row.get('tutor_name')) else None,
                'assigned_by': str(row.get('assigned_by', '')) if pd.notna(row.get('assigned_by')) else None,
                'active': convert_bool(row.get('active', True)),
            }
            
            # Handle dates
            if pd.notna(row.get('start_date')):
                try:
                    record['start_date'] = pd.to_datetime(row['start_date']).date().isoformat()
                except:
                    pass
            
            if pd.notna(row.get('end_date')):
                try:
                    record['end_date'] = pd.to_datetime(row['end_date']).date().isoformat()
                except:
                    pass
            
            records.append(record)
        
        if records:
            try:
                response = supabase.table('shift_assignments').upsert(records).execute()
                logger.info(f"[SUCCESS] Imported {len(records)} shift assignments")
                return len(records)
            except Exception as e:
                logger.error(f"Error inserting shift assignments: {e}")
                return 0
        return 0
        
    except Exception as e:
        logger.error(f"Error importing shift assignments: {e}")
        return 0

def import_availability():
    """Import availability from availability.csv"""
    csv_file = DATA_DIR / "availability.csv"
    if not csv_file.exists():
        logger.warning(f"File not found: {csv_file}")
        return 0
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Found {len(df)} availability records to import")
        
        records = []
        for _, row in df.iterrows():
            record = {
                'availability_id': str(row.get('availability_id', '')),
                'tutor_id': str(row.get('tutor_id', '')) if pd.notna(row.get('tutor_id')) else None,
                'day_of_week': str(row.get('day_of_week', '')),
                'start_time': str(row.get('start_time', '')) if pd.notna(row.get('start_time')) else None,
                'end_time': str(row.get('end_time', '')) if pd.notna(row.get('end_time')) else None,
                'is_recurring': convert_bool(row.get('is_recurring', True)),
                'slot_status': str(row.get('slot_status', 'available')).lower() if pd.notna(row.get('slot_status')) else 'available',
            }
            
            # Handle dates
            if pd.notna(row.get('effective_date')):
                try:
                    record['effective_date'] = pd.to_datetime(row['effective_date']).date().isoformat()
                except:
                    pass
            
            if pd.notna(row.get('end_date')):
                try:
                    record['end_date'] = pd.to_datetime(row['end_date']).date().isoformat()
                except:
                    pass
            
            records.append(record)
        
        if records:
            try:
                response = supabase.table('availability').upsert(records).execute()
                logger.info(f"[SUCCESS] Imported {len(records)} availability records")
                return len(records)
            except Exception as e:
                logger.error(f"Error inserting availability: {e}")
                return 0
        return 0
        
    except Exception as e:
        logger.error(f"Error importing availability: {e}")
        return 0

def import_time_slots():
    """Import time slots from time_slots.csv"""
    csv_file = DATA_DIR / "time_slots.csv"
    if not csv_file.exists():
        logger.warning(f"File not found: {csv_file}")
        return 0
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Found {len(df)} time slots to import")
        
        records = []
        for _, row in df.iterrows():
            record = {
                'slot_id': str(row.get('slot_id', '')),
                'tutor_id': str(row.get('tutor_id', '')) if pd.notna(row.get('tutor_id')) else None,
                'start_time': str(row.get('start_time', '')) if pd.notna(row.get('start_time')) else None,
                'end_time': str(row.get('end_time', '')) if pd.notna(row.get('end_time')) else None,
                'status': str(row.get('status', 'available')).lower() if pd.notna(row.get('status')) else 'available',
                'appointment_id': str(row.get('appointment_id', '')) if pd.notna(row.get('appointment_id')) else None,
            }
            
            # Handle date
            if pd.notna(row.get('date')):
                try:
                    record['date'] = pd.to_datetime(row['date']).date().isoformat()
                except:
                    pass
            
            records.append(record)
        
        if records:
            try:
                response = supabase.table('time_slots').upsert(records).execute()
                logger.info(f"[SUCCESS] Imported {len(records)} time slots")
                return len(records)
            except Exception as e:
                logger.error(f"Error inserting time slots: {e}")
                return 0
        return 0
        
    except Exception as e:
        logger.error(f"Error importing time slots: {e}")
        return 0

def import_tutor_courses():
    """Import tutor-course relationships from tutor_courses.csv"""
    csv_file = DATA_DIR / "tutor_courses.csv"
    if not csv_file.exists():
        logger.warning(f"File not found: {csv_file}")
        return 0
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Found {len(df)} tutor-course relationships to import")
        
        records = []
        for _, row in df.iterrows():
            record = {
                'tutor_id': str(row.get('tutor_id', '')) if pd.notna(row.get('tutor_id')) else None,
                'course_id': str(row.get('course_id', '')) if pd.notna(row.get('course_id')) else None,
            }
            if record['tutor_id'] and record['course_id']:
                records.append(record)
        
        if records:
            try:
                response = supabase.table('tutor_courses').upsert(records).execute()
                logger.info(f"[SUCCESS] Imported {len(records)} tutor-course relationships")
                return len(records)
            except Exception as e:
                logger.error(f"Error inserting tutor_courses: {e}")
                return 0
        return 0
        
    except Exception as e:
        logger.error(f"Error importing tutor_courses: {e}")
        return 0

def import_audit_log():
    """Import audit log from audit_log.csv"""
    csv_file = DATA_DIR / "audit_log.csv"
    if not csv_file.exists():
        logger.warning(f"File not found: {csv_file}")
        return 0
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Found {len(df)} audit log entries to import")
        
        records = []
        for _, row in df.iterrows():
            record = {
                'log_id': str(row.get('log_id', '')),
                'user_id': str(row.get('user_id', '')) if pd.notna(row.get('user_id')) else None,
                'user_email': str(row.get('user_email', '')) if pd.notna(row.get('user_email')) else None,
                'action': str(row.get('action', '')),
                'resource_type': str(row.get('resource_type', '')) if pd.notna(row.get('resource_type')) else None,
                'resource_id': str(row.get('resource_id', '')) if pd.notna(row.get('resource_id')) else None,
                'details': str(row.get('details', '')) if pd.notna(row.get('details')) else None,
                'ip_address': str(row.get('ip_address', '')) if pd.notna(row.get('ip_address')) else None,
            }
            
            # Handle timestamp
            if pd.notna(row.get('timestamp')):
                try:
                    record['timestamp'] = convert_datetime(row['timestamp'])
                except:
                    pass
            
            records.append(record)
        
        if records:
            try:
                response = supabase.table('audit_log').upsert(records).execute()
                logger.info(f"[SUCCESS] Imported {len(records)} audit log entries")
                return len(records)
            except Exception as e:
                logger.error(f"Error inserting audit_log: {e}")
                return 0
        return 0
        
    except Exception as e:
        logger.error(f"Error importing audit_log: {e}")
        return 0

def main():
    """Main import function - imports data in correct order"""
    print("=" * 60)
    print("CSV TO SUPABASE DATA IMPORT")
    print("=" * 60)
    print(f"Supabase URL: {SUPABASE_URL[:30]}...")
    print(f"Data directory: {DATA_DIR.absolute()}")
    print("\nImporting data in order (respecting foreign key dependencies)...\n")
    
    results = {}
    
    # Import in order (respecting dependencies)
    print("\n[1/10] Importing users...")
    results['users'] = import_users()
    
    print("\n[2/10] Importing tutors...")
    results['tutors'] = import_tutors()
    
    print("\n[3/10] Importing courses...")
    results['courses'] = import_courses()
    
    print("\n[4/10] Importing appointments...")
    results['appointments'] = import_appointments()
    
    print("\n[5/10] Importing shifts...")
    results['shifts'] = import_shifts()
    
    print("\n[6/10] Importing shift assignments...")
    results['shift_assignments'] = import_shift_assignments()
    
    print("\n[7/10] Importing availability...")
    results['availability'] = import_availability()
    
    print("\n[8/10] Importing time slots...")
    results['time_slots'] = import_time_slots()
    
    print("\n[9/10] Importing tutor-course relationships...")
    results['tutor_courses'] = import_tutor_courses()
    
    print("\n[10/10] Importing audit log...")
    results['audit_log'] = import_audit_log()
    
    # Summary
    print("\n" + "=" * 60)
    print("IMPORT SUMMARY")
    print("=" * 60)
    total = 0
    for table, count in results.items():
        print(f"  {table:20s}: {count:5d} records")
        total += count
    print(f"\n  {'TOTAL':20s}: {total:5d} records")
    print("=" * 60)
    print("\n[SUCCESS] Data import completed!")
    print("\nNext steps:")
    print("1. Verify data in Supabase Dashboard > Table Editor")
    print("2. Test your application")
    print("3. Run: python check_supabase_details.py")

if __name__ == "__main__":
    main()

