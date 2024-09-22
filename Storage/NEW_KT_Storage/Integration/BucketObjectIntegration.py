from datetime import datetime
import unittest
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..')))
from Storage.NEW_KT_Storage.Controller.BucketObjectController import BucketObjectController
from Storage.NEW_KT_Storage.Service.Classes.BucketObjectService import BucketObjectService
from Storage.NEW_KT_Storage.Test.BucketObjectTests import TestBucketObjectService

# demostrate all object functionallity

print('''---------------------Start Of session----------------------''')
start_time_session = datetime.now()
print(start_time_session)
print(f"{start_time_session} deonstration of object bucket_object start")

object_controller = BucketObjectController()

 # create
bucket_name = 'example_bucket'
object_key = 'example_object'
content = 'An example of the contents of a bucket object'
start_time= datetime.now()
print(f"{start_time} start creating object names {object_key}")
object_controller.create_bucket_object(bucket_name, object_key, content)
end_time=datetime.now()
print(f"{end_time} object {object_key} created successfully")
total_duration = end_time-start_time
print(total_duration)

suite = unittest.TestSuite()
suite.addTest(TestBucketObjectService('test_create_object'))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)


# get
start_time= datetime.now()
print(f"{start_time} start getting object named {object_key}")
received_object=object_controller.get_bucket_object(bucket_name, object_key)
end_time=datetime.now()
print(f"{end_time} object {object_key} was received successfully")
total_duration = end_time-start_time
print(end_time)
print(total_duration)

suite = unittest.TestSuite()
suite.addTest(TestBucketObjectService('test_get_object'))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# put
updated_content = 'new content for updated bucket object'
start_time= datetime.now()
print(f"{start_time} start updating object named {object_key}")
updated_object =object_controller.put_bucket_object(bucket_name, object_key, updated_content)
end_time=datetime.now()
print(f"{end_time} object {object_key} was updated successfully ")
total_duration = end_time-start_time
print(total_duration)

suite = unittest.TestSuite()
suite.addTest(TestBucketObjectService('test_put_object'))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# List all objects
start_time= datetime.now()
print(f"{start_time} start listing all objects in bucket {bucket_name}")
all_objects = object_controller.list_all_objects(bucket_name)
end_time=datetime.now()
print(f"{end_time} all objects in {bucket_name} were recieved successfully ")
total_duration = end_time-start_time
print(total_duration)

suite = unittest.TestSuite()
suite.addTest(TestBucketObjectService('test_get_all_objects'))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)


 # delete
start_time= datetime.now()
print(f'{start_time} start deleting object {object_key}')
object_controller.delete_bucket_object(bucket_name,object_key)
end_time=datetime.now()
print(f'''{end_time} object {object_key} deleted successfully''')
print(total_duration)

suite = unittest.TestSuite()
suite.addTest(TestBucketObjectService('test_delete_object'))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

print(f'{datetime.now()} deonstration of object XXX ended successfully')
print('---------------------End Of session----------------------')
end_time = datetime.now()
total_duration = start_time_session - end_time
print(end_time)
