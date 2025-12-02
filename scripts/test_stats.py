import requests
import json

base_url = "http://127.0.0.1:5000"
login_url = f"{base_url}/auth/login"
stats_url = f"{base_url}/admin/api/dashboard-stats"

session = requests.Session()

# 1. Login
print(f"Logging in to {login_url}...")
login_data = {
    "email": "admin@university.edu",
    "password": "password"
}
response = session.post(login_url, data=login_data)
print(f"Login Status: {response.status_code}")

# 2. Get Stats
print(f"Fetching stats from {stats_url}...")
response = session.get(stats_url)
print(f"Stats Status: {response.status_code}")
print("Response Body:")
print(response.text)
