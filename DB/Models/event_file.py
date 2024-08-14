import time
# from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# class MyHandler(FileSystemEventHandler):
#     count=0
#     def __init__(self,path_file,replicas=[]) :
#         MyHandler.count+=1
#         self.count_id=MyHandler.count
#         self.path_file=path_file
#         self.replicas=replicas
#         super().__init__()
#     def on_modified(self, event):
#         if event.src_path ==self.path_file:
#             print(f'File {event.src_path} has been modified')
#             # כאן אתה יכול להוסיף את הפונקציה שתרצה להפעיל
#             perform_action(self.replicas,self.path_file,self.count_id)

# def perform_action(replicas,path_file,count):
#     print(f'perform_action:{count}')
#     count+=1
#     for replica in replicas:
#         with open( path_file, 'r') as file:
#             replica.white_from_file(file.readline())

# if __name__ == "__main__":
#     path = "./"
#     event_handler1 = MyHandler( "./file.txt")
#     event_handler2 = MyHandler( "./file.txt")
#     observer = Observer()
#     observer.schedule(event_handler1, path, recursive=False)
#     observer.schedule(event_handler2, path, recursive=False)
#     observer.start()
#     print("start:")
#     try:
#         while True:
#             time.sleep(1)
#             with open('./file.txt', 'a') as file:
#                 file.writelines("frame_header\n")
#     except KeyboardInterrupt:
#         print("fail")
#         observer.stop()
#     observer.join()

class MyHandler(FileSystemEventHandler):
    def __init__(self, instance):
        self.instance = instance
        super().__init__()

    def on_modified(self, event):
        if event.src_path == self.instance.path_file:
            print(f'File {event.src_path} has been modified {self.instance.db_instance_identifier}')
            self.instance.perform_action()
