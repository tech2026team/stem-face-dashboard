import pandas as pd
import os
from pathlib import Path
import hashlib
from datetime import datetime

DATA_DIR = Path('data/core')
LOGS_DIR = Path('logs')

def recover_data():
    print("Recovering data from logs...")
    
    schedules_file = LOGS_DIR / 'schedules.csv'
    if not schedules_file.exists():
        print("Error: logs/schedules.csv not found.")
        return

    try:
        df = pd.read_csv(schedules_file)
        print(f"Loaded {len(df)} rows from schedules.csv")
        
        # 1. Recover Tutors
        # Extract unique tutor_id and tutor_name
        tutors_data = df[['tutor_id', 'tutor_name']].drop_duplicates().dropna()
        
        print(f"Found {len(tutors_data)} unique tutors.")
        
        tutors_records = []
        users_records = []
        
        # Load existing users to avoid duplicates (admins)
        users_file = DATA_DIR / 'users.csv'
        existing_users = pd.DataFrame()
        if users_file.exists():
            existing_users = pd.read_csv(users_file)
        
        default_password_hash = "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
        now = datetime.now().isoformat()
        
        for _, row in tutors_data.iterrows():
            tutor_id = str(row['tutor_id'])
            name = str(row['tutor_name'])
            
            # Create User ID (use T ID as User ID for simplicity, or generate USERxxx)
            # Let's use USER + hash or just sequential if we can.
            # To be safe and consistent with previous, let's generate a USER ID.
            # Actually, previous restoration used USER0001 etc.
            # Let's just use T ID as User ID to avoid confusion? No, system expects USER prefix maybe.
            # Let's use "USER_" + tutor_id
            user_id = f"USER_{tutor_id}"
            email = f"{name.lower().replace(' ', '.')}@university.edu"
            
            # Tutor Record
            tutors_records.append({
                'tutor_id': tutor_id,
                'user_id': user_id,
                'bio': f"Experienced tutor: {name}",
                'specializations': "General Science", # Default
                'max_appointments_per_day': 5,
                'is_available': True,
                'joined_date': now
            })
            
            # User Record
            if not existing_users.empty and user_id in existing_users['user_id'].values:
                continue
                
            users_records.append({
                'user_id': user_id,
                'email': email,
                'password_hash': default_password_hash,
                'full_name': name,
                'role': 'tutor',
                'active': True,
                'created_at': now,
                'last_login': None
            })
            
        # Save Tutors
        tutors_df = pd.DataFrame(tutors_records)
        tutors_df.to_csv(DATA_DIR / 'tutors.csv', index=False)
        print(f"Restored {len(tutors_df)} tutors to tutors.csv")
        
        # Save Users (Append)
        if users_records:
            new_users_df = pd.DataFrame(users_records)
            if not existing_users.empty:
                updated_users = pd.concat([existing_users, new_users_df], ignore_index=True)
            else:
                updated_users = new_users_df
            updated_users.to_csv(users_file, index=False)
            print(f"Restored {len(users_records)} tutor users to users.csv")
            
        # 2. Recover Availability
        print(f"Columns in schedules.csv: {df.columns.tolist()}")
        if not df.empty:
            print(f"Sample row: {df.iloc[0].to_dict()}")
            
        availability_records = []
        
        # Identify date column
        date_col = 'day'
        if 'day' not in df.columns:
            # Try to find date-like column
            for col in df.columns:
                if 'date' in col.lower() or 'day' in col.lower():
                    date_col = col
                    break
        print(f"Using date column: {date_col}")
        
        # Group by tutor and day to find ranges
        # Actually, the file seems to list slots? Or shifts?
        # Let's assume each row is a slot or a shift.
        # We'll create availability records for each unique day/time combo.
        
        # We need 'day_of_week'. 'day' column might be date (2025-03-19).
        # We need to convert date to day_of_week.
        
        count = 0
        for _, row in df.iterrows():
            try:
                tutor_id = str(row['tutor_id'])
                date_str = str(row[date_col])
                start_time = str(row['start_time'])
                end_time = str(row['end_time'])
                
                # Parse date to get day of week
                # Handle potential formats
                try:
                    date_obj = pd.to_datetime(date_str)
                    day_of_week = date_obj.strftime('%A')
                except Exception as e:
                    print(f"Failed to parse date '{date_str}': {e}")
                    continue
                
                # Create availability
                avail_id = f"AVL_{tutor_id}_{count}"
                
                # Ensure times are HH:MM:SS
                if len(start_time) == 5: start_time += ":00"
                if len(end_time) == 5: end_time += ":00"
                
                availability_records.append({
                    'availability_id': avail_id,
                    'tutor_id': tutor_id,
                    'day_of_week': day_of_week,
                    'start_time': start_time,
                    'end_time': end_time,
                    'is_recurring': True,
                    'slot_status': 'available',
                    'effective_date': '2025-01-01',
                    'end_date': '2025-12-31'
                })
                count += 1
            except Exception as e:
                print(f"Skipping row due to error: {e}")
                
        # Deduplicate
        avail_df = pd.DataFrame(availability_records)
        if not avail_df.empty:
            # Drop duplicates based on tutor, day, start, end
            avail_df = avail_df.drop_duplicates(subset=['tutor_id', 'day_of_week', 'start_time', 'end_time'])
            # Re-assign IDs after dedupe
            avail_df['availability_id'] = [f"AVL_{i}" for i in range(len(avail_df))]
            
            avail_df.to_csv(DATA_DIR / 'availability.csv', index=False)
            print(f"Restored {len(avail_df)} availability records.")
        else:
            print("No availability found after processing.")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    recover_data()
