import sys
import os
import pandas as pd
import logging

# Add app directory to path
sys.path.append(os.getcwd())

logging.basicConfig(level=logging.INFO)

try:
    print("Importing SchedulingAnalytics...")
    from app.core.analytics import SchedulingAnalytics
    from app.core.scheduling_manager import SchedulingManager
    
    print("Initializing SchedulingManager...")
    manager = SchedulingManager(data_dir='data/core')
    print("SchedulingManager initialized successfully.")
    
    print("Initializing SchedulingAnalytics...")
    analytics = SchedulingAnalytics(data_dir='data/core')
    print("SchedulingAnalytics initialized successfully.")
    
    print("Getting summary stats...")
    stats = analytics.get_summary_stats()
    print("Summary stats retrieved successfully:")
    print(stats)
    
except Exception as e:
    print(f"CRITICAL ERROR: {e}")
    import traceback
    traceback.print_exc()
