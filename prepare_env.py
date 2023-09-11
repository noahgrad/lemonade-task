import os
import json
from datetime import datetime

# Create a folder for testing
test_folder = 'inbound_folder'
if not os.path.exists(test_folder):
    os.makedirs(test_folder)

# Generate sample vehicle_events JSON files
vehicle_events_sample = [
    {
        'vehicle_id': 'ebab5f787798416fb2b8afc1340d7a4e',
        'event_time': '2022-06-05T21:02:34.546Z',
        'event_source': 'mobile',
        'event_type': 'start_drive',
        'event_value': '-79.9074173,9.3560572',
        'event_extra_data': {
            'note': 'the device is working properly',
            'boot_time': 12
        }
    },
    {
        "vehicle_id": "ebae3f787798416fb2b8afc1340d7a6d",
        "event_time": "2022-06-06T00:02:34.546Z",
        "event_source": "device",
        "event_type": "door_open",
        "event_value": "driver_door",
        "event_extra_data:": {
            "note": "a rare event, that is usually caused byaccident",
            "emergency_call": True
    }
    }
]

filename = f'vehicle_events_{datetime.now().strftime("%Y%m%d%H%M%S")}.json'
with open(os.path.join(test_folder, filename), 'w') as f:
    json.dump({'vehicle_events': vehicle_events_sample}, f)

# Generate sample vehicle_status JSON files
vehicle_status_sample = [
    {
        "vehicle_id": "ebab5f787798416fb2b8afc1340d7a4e",
        "report_time": "2022-05-05T21:02:34.546Z",
        "status_source": "mobile",
        "status": "driving",
    },
    {
        "vehicle_id": "ebae3f787798416fb2b8afc1340d7a6d",
        "report_time": "2022-05-06T00:02:34.546Z",
        "status_source": "device",
        "status": "accident",
    },
    {
        "vehicle_id": "qbae3f787798416fb2b8afc1340ddf19",
        "report_time": "2022-05-09T00:02:34.546Z",
        "status_source": "police",
        "status": "garage",
    }
]

filename = f'vehicle_status_{datetime.now().strftime("%Y%m%d%H%M%S")}.json'
with open(os.path.join(test_folder, filename), 'w') as f:
    json.dump({'vehicle_status': vehicle_status_sample}, f)