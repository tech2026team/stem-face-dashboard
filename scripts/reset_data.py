import pandas as pd
import os
import shutil
from pathlib import Path

DATA_DIR = Path('data/core')

def reset_data():
    print("WARNING: This will delete all non-admin users and their data.")
    print("Preserving admins: ADMIN001, U1764693957")
    
    # 1. Reset Users
    users_file = DATA_DIR / 'users.csv'
    if users_file.exists():
        df = pd.read_csv(users_file)
        # Keep admins
        admins = df[df['user_id'].isin(['ADMIN001', 'U1764693957'])]
        admins.to_csv(users_file, index=False)
        print(f"Reset users.csv: Kept {len(admins)} admins, deleted {len(df) - len(admins)} users.")
    
    # 2. Reset Tutors (Delete all, since admins aren't tutors)
    tutors_file = DATA_DIR / 'tutors.csv'
    if tutors_file.exists():
        df = pd.read_csv(tutors_file)
        # Create empty DF with same columns
        empty_df = pd.DataFrame(columns=df.columns)
        empty_df.to_csv(tutors_file, index=False)
        print(f"Reset tutors.csv: Deleted {len(df)} tutors.")

    # 3. Reset Appointments (Delete all)
    apts_file = DATA_DIR / 'appointments.csv'
    if apts_file.exists():
        df = pd.read_csv(apts_file)
        empty_df = pd.DataFrame(columns=df.columns)
        empty_df.to_csv(apts_file, index=False)
        print(f"Reset appointments.csv: Deleted {len(df)} appointments.")

    # 4. Reset Dependent Tables (Delete all)
    dependent_files = [
        'availability.csv',
        'shift_assignments.csv',
        'shifts.csv', # Optional, but shifts are usually admin-created. Maybe keep? User said "delete all users... booked appointments". 
                      # Shifts are structure, not user data per se. But assignments are user data.
                      # Let's keep shifts, delete assignments.
        'time_slots.csv',
        'tutor_courses.csv'
    ]
    
    for fname in dependent_files:
        fpath = DATA_DIR / fname
        if fpath.exists():
            df = pd.read_csv(fpath)
            if fname == 'shifts.csv':
                # Keep shifts structure? Or wipe? 
                # "delete all users... booked appointments"
                # Shifts are like "Morning Shift", "Evening Shift". They are definitions.
                # Assignments link tutors to shifts.
                # I'll keep shifts definitions, but wipe assignments.
                print(f"Skipping reset of {fname} (definitions).")
                continue
                
            empty_df = pd.DataFrame(columns=df.columns)
            empty_df.to_csv(fpath, index=False)
            print(f"Reset {fname}: Deleted {len(df)} records.")

if __name__ == "__main__":
    reset_data()
