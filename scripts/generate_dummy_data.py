import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import random
import hashlib

DATA_DIR = Path('data/core')

def generate_dummy_data():
    print("Generating dummy data...")
    
    # 1. Create Dummy Students
    students = [
        {'name': 'Alice Johnson', 'email': 'alice.j@university.edu'},
        {'name': 'Bob Smith', 'email': 'bob.s@university.edu'},
        {'name': 'Charlie Brown', 'email': 'charlie.b@university.edu'},
        {'name': 'Diana Prince', 'email': 'diana.p@university.edu'},
        {'name': 'Evan Wright', 'email': 'evan.w@university.edu'}
    ]
    
    users_file = DATA_DIR / 'users.csv'
    existing_users = pd.read_csv(users_file)
    
    new_users = []
    default_hash = "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
    now = datetime.now().isoformat()
    
    student_emails = []
    
    for i, s in enumerate(students):
        user_id = f"STU{i+1:04d}"
        if user_id not in existing_users['user_id'].values:
            new_users.append({
                'user_id': user_id,
                'email': s['email'],
                'password_hash': default_hash,
                'full_name': s['name'],
                'role': 'student',
                'active': True,
                'created_at': now,
                'last_login': None
            })
            student_emails.append(s['email'])
        else:
            # If exists, just add email to list
            student_emails.append(s['email'])
            
    if new_users:
        new_users_df = pd.DataFrame(new_users)
        updated_users = pd.concat([existing_users, new_users_df], ignore_index=True)
        updated_users.to_csv(users_file, index=False)
        print(f"Added {len(new_users)} students to users.csv")
    else:
        print("Students already exist.")

    # 2. Create Appointments based on Availability
    avail_file = DATA_DIR / 'availability.csv'
    if not avail_file.exists():
        print("No availability found.")
        return
        
    avail_df = pd.read_csv(avail_file)
    courses_df = pd.read_csv(DATA_DIR / 'courses.csv')
    course_ids = courses_df['course_id'].tolist()
    
    appointments = []
    
    # Generate appointments for next 7 days
    today = datetime.now().date()
    
    count = 0
    target_appointments = 15
    
    # Shuffle availability to randomize
    avail_df = avail_df.sample(frac=1).reset_index(drop=True)
    
    for _, row in avail_df.iterrows():
        if count >= target_appointments:
            break
            
        tutor_id = row['tutor_id']
        day_of_week = row['day_of_week']
        start_str = row['start_time']
        end_str = row['end_time'] # e.g. 14:00:00
        
        # Find next date for this day of week
        # 0=Monday, 6=Sunday
        days_map = {
            'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 
            'Friday': 4, 'Saturday': 5, 'Sunday': 6
        }
        target_day_num = days_map.get(day_of_week)
        
        if target_day_num is None: continue
        
        current_day_num = today.weekday()
        days_ahead = (target_day_num - current_day_num) % 7
        if days_ahead == 0: days_ahead = 7 # Next week if today
        
        appt_date = today + timedelta(days=days_ahead)
        
        # Create 1 hour appointment at start time
        # Ensure start < end
        try:
            s_time = datetime.strptime(start_str, '%H:%M:%S')
            e_time = datetime.strptime(end_str, '%H:%M:%S')
            
            # If slot is > 1 hour, just take first hour
            appt_end_time = s_time + timedelta(hours=1)
            
            if appt_end_time <= e_time:
                student = random.choice(students)
                
                appt_id = f"APT{count+1:05d}"
                appointments.append({
                    'appointment_id': appt_id,
                    'tutor_id': tutor_id,
                    'student_name': student['name'],
                    'student_email': student['email'],
                    'course_id': random.choice(course_ids),
                    'appointment_date': appt_date.isoformat(),
                    'start_time': s_time.strftime('%H:%M:%S'),
                    'end_time': appt_end_time.strftime('%H:%M:%S'),
                    'status': 'scheduled',
                    'booking_type': 'student_booked',
                    'confirmation_status': 'pending',
                    'notes': 'Dummy appointment',
                    'created_at': now,
                    'updated_at': now
                })
                count += 1
        except:
            continue
            
    if appointments:
        appt_df = pd.DataFrame(appointments)
        # Append to appointments.csv (which should be empty or have headers)
        appt_file = DATA_DIR / 'appointments.csv'
        if appt_file.exists():
            existing_appts = pd.read_csv(appt_file)
            updated_appts = pd.concat([existing_appts, appt_df], ignore_index=True)
        else:
            updated_appts = appt_df
            
        updated_appts.to_csv(appt_file, index=False)
        print(f"Added {len(appointments)} appointments to appointments.csv")
    else:
        print("Could not generate appointments.")

if __name__ == "__main__":
    generate_dummy_data()
