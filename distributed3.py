from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from redis import Redis
from rq import Queue
import json
from worker_functions_mysql import insert_vehicle_event, insert_vehicle_status
import time
import subprocess  # Import subprocess to start RQ workers


# Initialize Redis
redis_conn = Redis()

# Initialize Queues
event_queue = Queue('vehicle_events', connection=redis_conn)
status_queue = Queue('vehicle_status', connection=redis_conn)

def parse_and_enqueue_vehicle_events(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    for event in data['vehicle_events']:
        event_queue.enqueue(insert_vehicle_event, event)

def parse_and_enqueue_vehicle_status(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    for status in data['vehicle_status']:
        status_queue.enqueue(insert_vehicle_status, status)

class Watcher:
    def __init__(self, directory_to_watch):
        self.DIRECTORY_TO_WATCH = directory_to_watch
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Observer Stopped")

class Handler(FileSystemEventHandler):
    @staticmethod
    def process(event):
        if event.is_directory:
            return None
        elif event.event_type == 'created':
            print(f"Received created event - {event.src_path}")
            if 'vehicle_events' in event.src_path:
                parse_and_enqueue_vehicle_events(event.src_path)
            elif 'vehicle_status' in event.src_path:
                parse_and_enqueue_vehicle_status(event.src_path)

    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)

if __name__ == '__main__':
    src_path = "/Users/noa.gradovitch/lemonade/distributed/inbound_folder"
    watcher = Watcher(src_path)
    watcher.run()

    # Start RQ Workers for 'vehicle_events' and 'vehicle_status' queues
    subprocess.Popen(["rq", "worker", "vehicle_events"])
    subprocess.Popen(["rq", "worker", "vehicle_status"])