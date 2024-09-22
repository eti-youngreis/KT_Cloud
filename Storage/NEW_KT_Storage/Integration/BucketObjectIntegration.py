from datetime import datetime
import unittest
import os
import sys
import time

# Add the project path to allow importing the correct files
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

# Import the controller and the tests
from Storage.NEW_KT_Storage.Controller.BucketObjectController import BucketObjectController
from Storage.NEW_KT_Storage.Test.BucketObjectTests import TestBucketObjectService

# Start the session
print('''---------------------Start Of Session----------------------''')
start_time_session = datetime.now()
print(start_time_session)
print(f"{start_time_session} demonstration of bucket object functionality start")

# Create an instance of BucketObjectController
object_controller = BucketObjectController()

# Define the object and bucket details
bucket_name = 'example_bucket'
object_key = 'example_object'
content = 'An example of the contents of a bucket object'

# Create an object in the bucket
start_time = datetime.now()
print(f"{start_time} start creating object named {object_key}")
object_controller.create_bucket_object(bucket_name, object_key, content)
end_time = datetime.now()
print(f"{end_time} object {object_key} created successfully")
total_duration = end_time - start_time
print("The total duration for creating object: ", total_duration)

# Run test for object creation
suite = unittest.TestSuite()
suite.addTest(TestBucketObjectService('test_create_object'))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# Delay for 5 seconds
time.sleep(5)

# Retrieve the object
start_time = datetime.now()
print(f"{start_time} start getting object named {object_key}")
received_object = object_controller.get_bucket_object(bucket_name, object_key)
end_time = datetime.now()
print(f"{end_time} object {object_key} was received successfully")
total_duration = end_time - start_time
print("The total duration for getting object: ", total_duration)

# Run test for retrieving the object
suite = unittest.TestSuite()
suite.addTest(TestBucketObjectService('test_get_object'))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# Update the object with new content
updated_content = 'new content for updated bucket object'
start_time = datetime.now()
print(f"{start_time} start updating object named {object_key}")
object_controller.put_bucket_object(bucket_name, object_key, updated_content)
end_time = datetime.now()
print(f"{end_time} object {object_key} was updated successfully")
total_duration = end_time - start_time
print("The total duration for updating object: ", total_duration)

# Run test for updating the object
suite = unittest.TestSuite()
suite.addTest(TestBucketObjectService('test_put_object'))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# List all objects in the bucket
start_time = datetime.now()
print(f"{start_time} start listing all objects in bucket {bucket_name}")
all_objects = object_controller.list_all_objects(bucket_name)
end_time = datetime.now()
print(f"{end_time} all objects in {bucket_name} were listed successfully")
total_duration = end_time - start_time
print("The total duration for listing all objects: ", total_duration)

# Run test for listing all objects
suite = unittest.TestSuite()
suite.addTest(TestBucketObjectService('test_get_all_objects'))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# Delete the object from the bucket
start_time = datetime.now()
print(f'{start_time} start deleting object {object_key}')
object_controller.delete_bucket_object(bucket_name, object_key)
end_time = datetime.now()
print(f'{end_time} object {object_key} deleted successfully')
total_duration = end_time - start_time
print("The total duration for deleting object: ", total_duration)

# Run test for object deletion
suite = unittest.TestSuite()
suite.addTest(TestBucketObjectService('test_delete_object'))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# Attempt to retrieve the deleted object to test error handling
try:
    received_object = object_controller.get_bucket_object(bucket_name, object_key)
except Exception as e:
    print(e)

# End of session
print(f'{datetime.now()} demonstration of object ended successfully')
print('---------------------End Of Session----------------------')
end_time_session = datetime.now()
total_session_duration = end_time_session - start_time_session
print("Total session duration: ", total_session_duration)
