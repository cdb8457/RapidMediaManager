import requests
import json
import sys

# Rapid Media Manager Engine Webhook URL
WEBHOOK_URL = "http://localhost:5049/api/v1/engine/seerr_webhook"

# Mock Seerr Webhook Payload for a Movie (The Matrix: TMDB 603)
mock_payload = {
    "notification_type": "TEST_NOTIFICATION",
    "event": "MEDIA_APPROVED",
    "subject": "The Matrix",
    "message": "Your request for The Matrix has been approved.",
    "media": {
        "media_type": "movie",
        "tmdbId": 603,
        "tvdbId": None,
        "status": 3,
        "status4k": 1
    },
    "request": {
        "request_id": 123,
        "requestedBy_email": "user@example.com",
        "requestedBy_username": "admin"
    }
}

print("==================================================")
print("🎬 SEERR BRIDGE TEST SCRIPT")
print("==================================================")
print(f"Sending Mock Seerr Webhook to {WEBHOOK_URL}")
print(f"Requesting TMDB ID: {mock_payload['media']['tmdbId']} ({mock_payload['subject']})\n")

try:
    response = requests.post(WEBHOOK_URL, json=mock_payload, timeout=5)
    
    print(f"Status Code: {response.status_code}")
    print(f"Response Body: {json.dumps(response.json(), indent=2)}")
    
    if response.status_code == 200:
        print("\n✅ TEST SUCCESS: SEERR BRIDGE IS ACTIVE!")
        print("The backend successfully caught the Seerr webhook and processed the TMDB ID.")
    else:
        print("\n❌ TEST FAILED: Backend returned an error.")
        
except requests.exceptions.ConnectionError:
    print("❌ CONNECTION ERROR: Is the Rapid Media Manager backend running?")
    print("Please ensure you have started the backend using: uv run python media_manager/main.py")
except Exception as e:
    print(f"❌ TEST FAILED: {e}")
