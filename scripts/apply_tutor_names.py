import pandas as pd
import os
import glob

def apply_tutor_names():
    tutor_names = {}
    
    # 1. Find names from logs and data
    search_paths = ['logs/*.csv', 'data/core/*.csv', 'data/legacy/*.csv']
    
    print("Scanning logs for names...")
    for path_pattern in search_paths:
        for file_path in glob.glob(path_pattern):
            try:
                df = pd.read_csv(file_path)
                if 'tutor_id' in df.columns:
                    name_col = None
                    for col in df.columns:
                        if 'name' in col.lower() and 'tutor' in col.lower():
                            name_col = col
                            break
                    
                    if name_col:
                        for _, row in df.iterrows():
                            tid = str(row['tutor_id'])
                            name = str(row[name_col])
                            if tid and name and name.lower() != 'nan' and 'tutor' not in name.lower():
                                tutor_names[tid] = name
            except:
                pass
    
    print(f"Found {len(tutor_names)} unique tutor names.")
    
    # 2. Load mappings
    tutors_file = 'data/core/tutors.csv'
    users_file = 'data/core/users.csv'
    
    if not os.path.exists(tutors_file) or not os.path.exists(users_file):
        print("Error: Core data files not found.")
        return

    tutors_df = pd.read_csv(tutors_file)
    users_df = pd.read_csv(users_file)
    
    # Map tutor_id -> user_id
    tutor_to_user = dict(zip(tutors_df['tutor_id'].astype(str).str.strip(), tutors_df['user_id'].astype(str).str.strip()))
    
    updated_count = 0
    
    # Debug output to file
    with open('debug_names.txt', 'w') as f:
        f.write(f"Found {len(tutor_names)} names\n")
        if tutor_names:
            k = list(tutor_names.keys())[0]
            f.write(f"Sample log key: {repr(k)} type: {type(k)}\n")
        if tutor_to_user:
            k = list(tutor_to_user.keys())[0]
            f.write(f"Sample tutor key: {repr(k)} type: {type(k)}\n")
            
        test_id = 'T5060917'
        f.write(f"Is {test_id} in logs? {test_id in tutor_names}\n")
        f.write(f"Is {test_id} in tutors? {test_id in tutor_to_user}\n")
        
        # Dump some keys
        f.write("\nLog keys sample:\n")
        for k in list(tutor_names.keys())[:5]:
            f.write(f"{repr(k)}\n")
            
        f.write("\nTutor keys sample:\n")
        for k in list(tutor_to_user.keys())[:5]:
            f.write(f"{repr(k)}\n")

    for tid, name in tutor_names.items():
        tid = tid.strip()
        if tid in tutor_to_user:
            uid = tutor_to_user[tid]
            # Find user row
            mask = users_df['user_id'].astype(str) == uid
            if mask.any():
                users_df.loc[mask, 'full_name'] = name
                updated_count += 1
            else:
                pass # print(f"User ID {uid} not found in users.csv")
        else:
            pass
    
    # Save
    users_df.to_csv(users_file, index=False)
    print(f"Updated {updated_count} user records with real names.")

if __name__ == "__main__":
    apply_tutor_names()
