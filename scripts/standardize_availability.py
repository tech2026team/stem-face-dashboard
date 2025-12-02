import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

DATA_DIR = Path('data/core')

def round_time_to_hour(time_str):
    """Round time string to nearest hour"""
    try:
        t = pd.to_datetime(time_str, format='%H:%M:%S').time()
        
        # Convert to minutes
        minutes = t.hour * 60 + t.minute
        
        # Round to nearest 60
        remainder = minutes % 60
        if remainder >= 30:
            minutes += (60 - remainder)
        else:
            minutes -= remainder
            
        # Convert back to HH:MM:SS
        hours = minutes // 60
        # Handle overflow (e.g. 24:00 -> 00:00? Or keep as 24? 
        # Time object can't hold 24. Let's cap at 23:00 or wrap.
        # But availability usually implies within same day.
        # If 23:45 rounds to 24:00, that's 00:00 next day.
        # Let's handle 24 as 23:59:59 or just 23:00 if it's end time?
        # Simpler: just round.
        
        if hours >= 24:
            hours = 23 # Cap at 23 to stay in day
            
        return f"{hours:02d}:00:00"
    except:
        return time_str

def standardize_availability():
    print("Standardizing availability to full hours...")
    
    csv_file = DATA_DIR / 'availability.csv'
    if not csv_file.exists():
        print("availability.csv not found")
        return

    df = pd.read_csv(csv_file)
    
    # Apply rounding
    df['start_time'] = df['start_time'].apply(round_time_to_hour)
    df['end_time'] = df['end_time'].apply(round_time_to_hour)
    
    # Ensure start < end
    # If start == end after rounding (e.g. 12:15 -> 12:00, 12:45 -> 13:00? No, 12:45 -> 13:00.
    # If 12:15 -> 12:00 and 12:45 -> 13:00, duration is 1h.
    # If 12:20 -> 12:00 and 12:40 -> 13:00, duration is 1h.
    # If 12:00 -> 12:00, invalid.
    
    # Filter out invalid slots
    valid_mask = df['start_time'] < df['end_time']
    invalid_count = len(df) - valid_mask.sum()
    
    df = df[valid_mask]
    
    if invalid_count > 0:
        print(f"Removed {invalid_count} slots that became invalid (start >= end) after rounding.")
        
    # Save
    df.to_csv(csv_file, index=False)
    print(f"Updated {len(df)} availability records.")
    print("Sample:")
    print(df[['start_time', 'end_time']].head())

if __name__ == "__main__":
    standardize_availability()
