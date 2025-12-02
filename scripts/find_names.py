import pandas as pd
import os
import glob

def find_tutor_names():
    tutor_names = {}
    
    # Search all CSVs in logs and data
    search_paths = ['logs/*.csv', 'data/core/*.csv', 'data/legacy/*.csv']
    
    for path_pattern in search_paths:
        for file_path in glob.glob(path_pattern):
            try:
                df = pd.read_csv(file_path)
                
                # Check for tutor_id and name columns
                if 'tutor_id' in df.columns:
                    name_col = None
                    for col in df.columns:
                        if 'name' in col.lower() and 'tutor' in col.lower():
                            name_col = col
                            break
                        elif 'full_name' in col.lower():
                            name_col = col
                            break
                    
                    if name_col:
                        print(f"Found names in {file_path} using column {name_col}")
                        for _, row in df.iterrows():
                            tid = str(row['tutor_id'])
                            name = str(row[name_col])
                            if tid and name and name.lower() != 'nan' and 'tutor' not in name.lower():
                                tutor_names[tid] = name
            except Exception as e:
                pass

    print(f"Found {len(tutor_names)} unique tutor names.")
    for tid, name in tutor_names.items():
        print(f"{tid}: {name}")

    # If we found names, let's update users.csv
    if tutor_names:
        users_file = 'data/core/users.csv'
        if os.path.exists(users_file):
            users_df = pd.read_csv(users_file)
            updated_count = 0
            
            for index, row in users_df.iterrows():
                # Find tutor_id for this user (assuming we can link them)
                # In users.csv we don't have tutor_id directly, but we have email or we can infer
                # Wait, my previous script put tutor_id in user_id for tutors (e.g. T123)
                # No, previous script put user_id as T123? No, it put user_id as USER0001 etc.
                # But tutors.csv maps USER0001 -> T123.
                
                # We need to load tutors.csv to map user_id -> tutor_id
                pass
            
            # Let's just print the mapping for now, I will write a better update script once I see the data
            
if __name__ == "__main__":
    find_tutor_names()
