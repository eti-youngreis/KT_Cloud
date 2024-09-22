import pytest
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

os.chdir("C:/Users/shana/Desktop/git/kT_Cloud")

from Controller.MultipartUploadController import MultipartUploadController
from datetime import datetime

# demonstrate all object functionallity
multipartUploadController=MultipartUploadController()
print('''---------------------Start Of session----------------------''')
start_time_session=datetime.now()

print(f'{start_time_session} deonstration of object multipart start')

# create
start_time= datetime.now()
print(f"{start_time} going to create multipart for object name example")
upload_id=multipartUploadController.initiate_upload('my_bucket','example3.txt')
end_time=datetime.now()
print(f"{end_time} multipart for object name example created successfully with upload_id {upload_id}")
total_duration = end_time - start_time
print(total_duration)

#upload_file_parts
start_time= datetime.now()
print(f"{start_time} going to upload file parts for object name example")
multipartUploadController.upload_file_parts(upload_id,"C:/Users/shana/Desktop/a/my_file.txt")
end_time=datetime.now()
print(f"{end_time} file parts for object name example uploaded successfully")
total_duration = end_time - start_time
print(total_duration)

#list_parts
start_time= datetime.now()
print(f"{start_time} going to list parts for object name example")
list_parts=multipartUploadController.list_parts(upload_id)
end_time=datetime.now()
print(f"{end_time} list parts for object name example listed successfully {list_parts}")
total_duration = end_time - start_time
print(total_duration)

# # comlete_file_parts
# start_time= datetime.now()
# print(f"{start_time} going to complete file parts for object name example")
# multipartUploadController.complete_upload(upload_id)
# end_time=datetime.now()
# print(f"{end_time} file parts for object name example completed successfully")
# total_duration = end_time - start_time
# print(total_duration)

# #abort_file_parts
# start_time= datetime.now()
# print(f"{start_time} going to abort file parts for object name example")
# multipartUploadController.abort_multipart_upload(upload_id)
# end_time=datetime.now()
# print(f"{end_time} file parts for object name example aborted successfully")
# total_duration = end_time - start_time
# print(total_duration)

#tests
print("running test for functions")
pytest.main(['-v', 'Storage/NEW_KT_Storage/Test/test_multipart_upload.py'])

end_time_session=datetime.now()
print(f'{end_time_session} deonstration of object multipart upload ended successfully')
print('''---------------------End Of session----------------------''')
total_duration = end_time_session - start_time_session
print(total_duration)
