import pandas as pd
import hashlib
from datetime import datetime

users_file = 'data/core/users.csv'
tutors_file = 'data/core/tutors.csv'

# Load files
try:
    users_df = pd.read_csv(users_file)
    tutors_df = pd.read_csv(tutors_file)
except Exception as e:
    print(f"Error loading files: {e}")
    exit(1)

# Get existing user IDs
existing_user_ids = set(users_df['user_id'].astype(str))

# Prepare new users list
new_users = []
default_password_hash = "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8" # "password"
now = datetime.now().isoformat()

for _, tutor in tutors_df.iterrows():
    user_id = str(tutor['user_id'])
    
    if user_id not in existing_user_ids:
        # Generate mock user data
        tutor_num = user_id.replace('USER', '')
        email = f"tutor{tutor_num}@university.edu"
        full_name = f"Tutor {tutor_num}"
        
        # Try to make name more realistic if possible (optional)
        # But for now, generic is fine to fix the "Unknown" issue immediately
        
        new_user = {
            'user_id': user_id,
            'email': email,
            'password_hash': default_password_hash,
            'full_name': full_name,
            'role': 'tutor',
            'active': True,
            'created_at': now,
            'last_login': None
        }
        new_users.append(new_user)

if new_users:
    print(f"Restoring {len(new_users)} missing tutor users...")
    new_users_df = pd.DataFrame(new_users)
    
    # Append to existing users
    updated_users_df = pd.concat([users_df, new_users_df], ignore_index=True)
    
    # Save back to CSV
    updated_users_df.to_csv(users_file, index=False)
    print("Successfully restored missing users.")
else:
    print("No missing users found.")
