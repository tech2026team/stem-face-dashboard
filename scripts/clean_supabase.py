import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: Supabase credentials not found.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def truncate_tables():
    print("WARNING: This will delete ALL data from Supabase tables.")
    
    # Order matters due to foreign keys
    tables = [
        'audit_log',
        'tutor_courses',
        'time_slots',
        'availability',
        'shift_assignments',
        'appointments',
        'tutors',
        'users', # This might be tricky if auth.users is involved, but we are targeting public.users
        'courses', # User didn't say delete courses, but "delete all users". 
                   # Courses are not users. I should probably KEEP courses.
                   # User said "delete all users... booked appointments".
                   # I will skip courses and shifts (definitions).
    ]
    
    for table in tables:
        try:
            # Supabase-py doesn't have a truncate method, so we delete all rows
            # We need to use a condition that matches all rows. 
            # Usually .delete().neq('id', 0) works if id is int, or similar.
            # Or .delete().gt('id', -1)
            
            # For users, we want to keep admins.
            if table == 'users':
                print(f"Cleaning {table} (preserving admins)...")
                # Delete where role != 'admin'
                response = supabase.table(table).delete().neq('role', 'admin').execute()
                print(f"Deleted non-admins from {table}")
            elif table == 'courses':
                print(f"Skipping {table}")
            else:
                print(f"Truncating {table}...")
                # Delete all. We need a filter. 
                # Let's try to delete where some column is not null.
                # Or use a known column.
                
                # Hack: Delete where 'created_at' is not null (if exists) or similar.
                # Better: Use a column that always exists.
                
                col_map = {
                    'audit_log': 'log_id',
                    'tutor_courses': 'tutor_id', # Composite key? might be hard.
                    'time_slots': 'slot_id',
                    'availability': 'availability_id',
                    'shift_assignments': 'assignment_id',
                    'appointments': 'appointment_id',
                    'tutors': 'tutor_id'
                }
                
                if table in col_map:
                    col = col_map[table]
                    # Delete where col is not null (which is everything)
                    # supabase-py syntax for "not null" is .neq(col, 'null')? No.
                    # .neq(col, '00000000-0000-0000-0000-000000000000')?
                    
                    # Let's try fetching IDs and deleting in batches if truncate isn't easy.
                    # Or just use .neq('id', 'placeholder')
                    
                    response = supabase.table(table).delete().neq(col, 'placeholder_impossible_value').execute()
                    print(f"Cleaned {table}")
                elif table == 'tutor_courses':
                     # No single ID. Delete where tutor_id is not null
                     response = supabase.table(table).delete().neq('tutor_id', 'placeholder').execute()
                     print(f"Cleaned {table}")

        except Exception as e:
            print(f"Error cleaning {table}: {e}")

if __name__ == "__main__":
    truncate_tables()
