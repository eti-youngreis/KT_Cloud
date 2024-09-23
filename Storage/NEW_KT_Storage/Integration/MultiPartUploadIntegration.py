import pytest
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
os.chdir("C:/Users/shana/Desktop/final-git/kT_Cloud")

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
upload_id=multipartUploadController.initiate_upload('my_bucket','new-example-final8.txt')
end_time=datetime.now()
print(f"{end_time} multipart for object name example created successfully with upload_id {upload_id}")
total_duration = end_time - start_time
print(total_duration)

time.sleep(5)

#upload_file_parts
start_time= datetime.now()
print(f"{start_time} going to upload file parts for object name example")
multipartUploadController.upload_file_parts(upload_id,"C:/Users/shana/Desktop/a/my_file.txt")
end_time=datetime.now()
print(f"{end_time} file parts for object name example uploaded successfully")
total_duration = end_time - start_time
print(total_duration)

time.sleep(5)


#list_parts
start_time= datetime.now()
print(f"{start_time} going to list parts for object name example")
list_parts=multipartUploadController.list_parts(upload_id)
end_time=datetime.now()
print(f"{end_time} list parts for object name example listed successfully {list_parts}")
total_duration = end_time - start_time
print(total_duration)


#abort_file_parts
start_time= datetime.now()
print(f"{start_time} going to abort file parts for object name example")
multipartUploadController.abort_multipart_upload(upload_id)
end_time=datetime.now()
print(f"{end_time} file parts for object name example aborted successfully")
total_duration = end_time - start_time
print(total_duration)

# #test for abort_file_parts
print("running test for function")
pytest.main(['-q', 'Storage/NEW_KT_Storage/Test/test_multipart_upload.py::test_abort_multipart_upload_invalid_upload_id'])

time.sleep(5)


# create
start_time= datetime.now()
print(f"{start_time} going to create multipart for object name example")
upload_id=multipartUploadController.initiate_upload('my_bucket','new-example-final8.txt')
end_time=datetime.now()
print(f"{end_time} multipart for object name example created successfully with upload_id {upload_id}")
total_duration = end_time - start_time
print(total_duration)

time.sleep(5)

#test for create
print("running test for function")
pytest.main(['-q', 'Storage/NEW_KT_Storage/Test/test_multipart_upload.py::test_initiate_multipart_upload'])

#upload_file_parts
start_time= datetime.now()
print(f"{start_time} going to upload file parts for object name example")
multipartUploadController.upload_file_parts(upload_id,"C:/Users/shana/Desktop/a/my_file.txt")
end_time=datetime.now()
print(f"{end_time} file parts for object name example uploaded successfully")
total_duration = end_time - start_time
print(total_duration)

time.sleep(5)

#test for upload_file_parts
print("running test for function")
pytest.main(['-q', 'Storage/NEW_KT_Storage/Test/test_multipart_upload.py::test_upload_file_parts'])

#list_parts
start_time= datetime.now()
print(f"{start_time} going to list parts for object name example")
list_parts=multipartUploadController.list_parts(upload_id)
end_time=datetime.now()
print(f"{end_time} list parts for object name example listed successfully {list_parts}")
total_duration = end_time - start_time
print(total_duration)

#test for list_parts
print("running test for function")
pytest.main(['-q', 'Storage/NEW_KT_Storage/Test/test_multipart_upload.py::test_list_parts_success'])

# comlete_file_parts
start_time= datetime.now()
print(f"{start_time} going to complete file parts for object name example")
multipartUploadController.complete_upload(upload_id)
end_time=datetime.now()
print(f"{end_time} file parts for object name example completed successfully")
total_duration = end_time - start_time
print(total_duration)

#test for comlete_file_parts
print("running test for function")
pytest.main(['-q', 'Storage/NEW_KT_Storage/Test/test_multipart_upload.py::test_complete_upload_missing_parts'])


end_time_session=datetime.now()
print(f'{end_time_session} deonstration of object multipart upload ended successfully')
print('''---------------------End Of session----------------------''')
total_duration = end_time_session - start_time_session
print(total_duration)
