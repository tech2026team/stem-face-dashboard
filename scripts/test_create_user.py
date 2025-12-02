import requests
import json
import time

base_url = "http://127.0.0.1:5000"
login_url = f"{base_url}/auth/login"
create_url = f"{base_url}/api/admin/create-user"

session = requests.Session()

# 1. Login
print(f"Logging in to {login_url}...")
login_data = {
    "email": "admin@university.edu",
    "password": "password"
}
response = session.post(login_url, data=login_data)
print(f"Login Status: {response.status_code}")

# 2. Create User
timestamp = int(time.time())
new_user = {
    "email": f"test_user_{timestamp}@example.com",
    "password": "password123",
    "full_name": f"Test User {timestamp}",
    "role": "tutor"
}

print(f"Creating user at {create_url}...")
response = session.post(create_url, json=new_user)
print(f"Create Status: {response.status_code}")
print("Response Body:")
print(response.text)
