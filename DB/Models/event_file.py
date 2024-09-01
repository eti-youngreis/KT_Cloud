import time
from watchdog.events import FileSystemEventHandler

class MyHandler(FileSystemEventHandler):
    def __init__(self, instance):
        self.instance = instance
        super().__init__()

    def on_modified(self, event):
        if event.src_path == self.instance.path_file:
            print(f'File {event.src_path} has been modified {self.instance.db_instance_identifier}')
            self.instance.perform_action()
