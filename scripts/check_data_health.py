import pandas as pd
import os
import glob

DATA_DIR = 'data/core'

def check_data_health():
    print(f"Checking data health in {DATA_DIR}...\n")
    
    files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    dfs = {}
    
    # 1. Load all files
    for f in files:
        name = os.path.basename(f)
        try:
            df = pd.read_csv(f)
            dfs[name] = df
            print(f"[{name}] Loaded {len(df)} rows.")
            
            # Check for NaN
            if df.isnull().values.any():
                null_counts = df.isnull().sum()
                print(f"  WARNING: Found NaN values:")
                for col, count in null_counts.items():
                    if count > 0:
                        print(f"    - {col}: {count} missing")
        except Exception as e:
            print(f"[{name}] ERROR loading: {e}")

    print("\n--- Integrity Checks ---\n")

    # 2. Check Users
    if 'users.csv' in dfs:
        users = dfs['users.csv']
        if 'active' not in users.columns and 'is_active' not in users.columns:
            print("[users.csv] CRITICAL: Missing 'active' status column")
        
        # Check for generic names if we care
        generic_names = users[users['full_name'].str.contains('Tutor \d+', na=False)]
        if not generic_names.empty:
            print(f"[users.csv] NOTE: {len(generic_names)} users still have generic names (e.g. 'Tutor 001')")

    # 3. Check Tutors -> Users link
    if 'tutors.csv' in dfs and 'users.csv' in dfs:
        tutors = dfs['tutors.csv']
        users = dfs['users.csv']
        
        # Check if all tutor user_ids exist in users.csv
        missing_users = tutors[~tutors['user_id'].isin(users['user_id'])]
        if not missing_users.empty:
            print(f"[tutors.csv] CRITICAL: {len(missing_users)} tutors have user_ids that don't exist in users.csv")
            print(f"  Missing IDs: {missing_users['user_id'].tolist()[:5]}...")
        else:
            print("[tutors.csv] OK: All tutors linked to valid users.")

    # 4. Check Appointments -> Tutors
    if 'appointments.csv' in dfs and 'tutors.csv' in dfs:
        apts = dfs['appointments.csv']
        tutors = dfs['tutors.csv']
        
        missing_tutors = apts[~apts['tutor_id'].isin(tutors['tutor_id'])]
        if not missing_tutors.empty:
            print(f"[appointments.csv] WARNING: {len(missing_tutors)} appointments have invalid tutor_ids")
        else:
            print("[appointments.csv] OK: All appointments linked to valid tutors.")

    # 5. Check Tutor Courses -> Tutors & Courses
    if 'tutor_courses.csv' in dfs:
        tc = dfs['tutor_courses.csv']
        
        if 'tutors.csv' in dfs:
            missing = tc[~tc['tutor_id'].isin(dfs['tutors.csv']['tutor_id'])]
            if not missing.empty:
                print(f"[tutor_courses.csv] WARNING: {len(missing)} records have invalid tutor_ids")
        
        if 'courses.csv' in dfs:
            missing = tc[~tc['course_id'].isin(dfs['courses.csv']['course_id'])]
            if not missing.empty:
                print(f"[tutor_courses.csv] WARNING: {len(missing)} records have invalid course_ids")

if __name__ == "__main__":
    check_data_health()
