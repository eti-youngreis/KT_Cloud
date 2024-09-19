import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from Storage.NEW_KT_Storage.Service.Classes.BucketObjectService import BucketObjectService
import unittest
from Storage.NEW_KT_Storage.Models.BucketObjectModel import BucketObject


class TestBucketObjectService(unittest.TestCase):
    def setUp(self):
        # Create a temporary path for the tests
        self.test_path = "C:\\Users\\user1\\Desktop\\server"
        os.makedirs(self.test_path, exist_ok=True)
        self.bucket_service = BucketObjectService(self.test_path)

        # Create a bucket directory
        self.bucket_name = "test_bucket"
        os.makedirs(os.path.join(self.test_path, self.bucket_name), exist_ok=True)

    def tearDown(self):
        # Cleanup after each test: remove all created files and directories
        for root, dirs, files in os.walk(self.test_path+"\\"+self.bucket_name, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.test_path+"\\"+self.bucket_name)

    def test_create_object(self):
        """Test creating a new object in a bucket"""
        object_key = "test_object.txt"
        content = "This is a test file."
        attributes = {
            'bucket_name': self.bucket_name,
            'object_key': object_key,
            'content': content
        }

        # Call the create method
        self.bucket_service.create(**attributes)

        # Check if the file was created physically
        full_path = os.path.join(self.test_path, self.bucket_name, object_key)
        self.assertTrue(os.path.exists(full_path))

        # Check if the content is correct
        with open(full_path, 'r') as f:
            file_content = f.read()
        self.assertEqual(file_content, content)
        self.bucket_service.delete(self.bucket_name,object_key)


    def test_delete_object(self):
        """Test deleting an object from a bucket"""
        object_key = "test_object.txt"
        content = "This is a test file."
        attributes = {
            'bucket_name': self.bucket_name,
            'object_key': object_key,
            'content': content
        }

        # Create the object first
        self.bucket_service.create(**attributes)

        # Delete the object
        self.bucket_service.delete(self.bucket_name, object_key)

        # Check if the file was deleted physically
        full_path = os.path.join(self.test_path, self.bucket_name, object_key)
        self.assertFalse(os.path.exists(full_path))

    def test_get_object(self):
        """Test retrieving an object from a bucket"""
        object_key = "test_object.txt"
        content = "This is a test file."
        attributes = {
            'bucket_name': self.bucket_name,
            'object_key': object_key,
            'content': content
        }

        # Create the object first
        self.bucket_service.create(**attributes)

        # Retrieve the object and verify the data
        bucket_object = self.bucket_service.get(self.bucket_name, object_key)
        self.assertIsInstance(bucket_object, BucketObject)
        self.assertEqual(bucket_object.bucket_name, self.bucket_name)
        # self.assertEqual(bucket_object["object_key"], object_key)
        # self.assertEqual(bucket_object,[(self.bucket_name+object_key, self.bucket_name, object_key, 'None', 'None')])
        self.bucket_service.delete(self.bucket_name,object_key)

    def test_get_all_objects(self):
        """Test retrieving all objects from a bucket"""
        object_key1 = "test_object1.txt"
        object_key2 = "test_object2.txt"
        content = "This is a test file."

        # Create two objects
        self.bucket_service.create(bucket_name=self.bucket_name, object_key=object_key1, content=content)
        self.bucket_service.create(bucket_name=self.bucket_name, object_key=object_key2, content=content)

        # Retrieve all objects
        all_objects = self.bucket_service.get_all(self.bucket_name)
        self.assertEqual(len(all_objects), 2)
        self.bucket_service.delete(self.bucket_name, object_key1)
        self.bucket_service.delete(self.bucket_name, object_key2)

    def test_put_object(self):
        """Test modifying an existing object in a bucket"""
        object_key = "test_object.txt"
        original_content = "This is the original content."
        updated_content = "This is the updated content."

        attributes = {
            'bucket_name': self.bucket_name,
            'object_key': object_key,
            'content': original_content
        }

        # Create the object first
        self.bucket_service.create(**attributes)

        # Update the object
        updated_attributes = {
            'bucket_name': self.bucket_name,
            'object_key': object_key,
            'content': updated_content
        }
        self.bucket_service.put(**updated_attributes)

        # Check if the content was updated
        full_path = os.path.join(self.test_path, self.bucket_name, object_key)
        with open(full_path, 'r') as f:
            file_content = f.read()
        self.assertEqual(file_content, updated_content)
        self.bucket_service.delete(self.bucket_name, object_key)


    def test_put_another_object(self):
        """add new object to bucket"""
        object_key = "test_object.txt"
        original_content = "This is the original content."
        updated_content = "This is the updated content."

        attributes = {
            'bucket_name': self.bucket_name,
            'object_key': object_key,
            'content': original_content
        }

        # Create the object first
        self.bucket_service.create(**attributes)

        new_object_key="second_test_object.txt"
        # Update the object
        updated_attributes = {
            'bucket_name': self.bucket_name,
            'object_key': new_object_key,
            'content': updated_content
        }
        self.bucket_service.put(**updated_attributes)
        all_objects = self.bucket_service.get_all(self.bucket_name)
        self.assertEqual(len(all_objects), 2)
        self.bucket_service.delete(self.bucket_name, object_key)
        self.bucket_service.delete(self.bucket_name, new_object_key)

    def test_create_object_invalid_bucket(self):
        """Test creating an object with an invalid bucket name"""
        object_key = "test_object.txt"
        content = "This is a test file."
        invalid_bucket_name = "invalid_bucket"
        attributes = {
            'bucket_name': invalid_bucket_name,
            'object_key': object_key,
            'content': content
        }

        with self.assertRaises(ValueError, msg="Bucket not found"):
            self.bucket_service.create(**attributes)

    def test_create_object_invalid_object_key(self):
        """Test creating an object with an invalid object key"""
        object_key = "invalid/key?"
        content = "This is a test file."
        attributes = {
            'bucket_name': self.bucket_name,
            'object_key': object_key,
            'content': content
        }

        with self.assertRaises(ValueError, msg="Incorrect object key"):
            self.bucket_service.create(**attributes)

    def test_delete_non_existent_object(self):
        """Test deleting a non-existent object"""
        object_key = "non_existent_object.txt"

        with self.assertRaises(ValueError, msg="Object not found"):
            self.bucket_service.delete(self.bucket_name, object_key)

    def test_get_non_existent_object(self):
        """Test retrieving a non-existent object"""
        object_key = "non_existent_object.txt"

        with self.assertRaises(ValueError, msg="Object not found"):
            self.bucket_service.get(self.bucket_name, object_key)

    def test_create_object_with_no_content(self):
        """Test creating an object with no content (empty file)"""
        object_key = "empty_object.txt"
        attributes = {
            'bucket_name': self.bucket_name,
            'object_key': object_key,
            'content': ""
        }

        # Call the create method
        self.bucket_service.create(**attributes)

        # Check if the file was created physically
        full_path = os.path.join(self.test_path, self.bucket_name, object_key)
        self.assertTrue(os.path.exists(full_path))

        # Check if the content is empty
        with open(full_path, 'r') as f:
            file_content = f.read()
        self.assertEqual(file_content, "")
        self.bucket_service.delete(self.bucket_name, object_key)

    def test_list_files_in_nonexistent_bucket(self):
        """Test retrieving all objects from a nonexistent bucket."""
        with self.assertRaises(ValueError) as context:
            self.bucket_service.get_all("nonexistent_bucket")

        self.assertTrue("Bucket not found" in str(context.exception))

    def test_create_object_in_nonexistent_bucket(self):
        """Test creating an object in a nonexistent bucket."""
        attributes = {
            'bucket_name': "nonexistent_bucket",
            'object_key': "test_object.txt",
            'content': "This is a test file."
        }

        with self.assertRaises(ValueError) as context:
            self.bucket_service.create(**attributes)

        self.assertTrue("Bucket not found" in str(context.exception))

    def test_get_object_from_empty_bucket(self):
        """Test retrieving an object from an empty bucket."""
        with self.assertRaises(ValueError) as context:
            self.bucket_service.get(self.bucket_name, "nonexistent_object.txt")

        self.assertTrue("Object not found" in str(context.exception))

    def test_delete_nonexistent_object(self):
        """Test deleting an object that doesn't exist."""
        with self.assertRaises(ValueError) as context:
            self.bucket_service.delete(self.bucket_name, "nonexistent_object.txt")

        self.assertTrue("Object not found" in str(context.exception))

    def test_overwrite_existing_object(self):
        """Test overwriting an existing object."""
        object_key = "test_object.txt"
        original_content = "Original content."
        updated_content = "Updated content."

        # Create the original object
        self.bucket_service.create(bucket_name=self.bucket_name, object_key=object_key, content=original_content)

        # Overwrite the object with new content
        self.bucket_service.put(bucket_name=self.bucket_name, object_key=object_key, content=updated_content)

        # Check if the file content was updated
        full_path = os.path.join(self.test_path, self.bucket_name, object_key)
        with open(full_path, 'r') as f:
            file_content = f.read()

        self.assertEqual(file_content, updated_content)
        self.bucket_service.delete(self.bucket_name,object_key)

    def test_get_object_from_empty_directory(self):
        """Test getting an object from an empty bucket."""
        object_key = "test_object.txt"

        with self.assertRaises(ValueError):
            self.bucket_service.get(self.bucket_name, object_key)

    def test_create_and_delete_multiple_objects(self):
        """Test creating and deleting multiple objects in a bucket."""
        object_keys = [f"test_object_{i}.txt" for i in range(5)]
        content = "This is a test file."

        # Create multiple objects
        for key in object_keys:
            self.bucket_service.create(bucket_name=self.bucket_name, object_key=key, content=content)

        # Verify that all objects exist
        for key in object_keys:
            full_path = os.path.join(self.test_path, self.bucket_name, key)
            self.assertTrue(os.path.exists(full_path))

        # Delete multiple objects
        for key in object_keys:
            self.bucket_service.delete(self.bucket_name, key)

        # Verify that all objects are deleted
        for key in object_keys:
            full_path = os.path.join(self.test_path, self.bucket_name, key)
            self.assertFalse(os.path.exists(full_path))

    def test_create_object_invalid_bucket_name(self):
        """Test creating an object with an invalid bucket name (short name)"""
        object_key = "invalid/key?"
        content = "This is a test file."
        attributes = {
            'bucket_name': 'hi',
            'object_key': object_key,
            'content': content
        }

        with self.assertRaises(ValueError, msg="Incorrect bucket name"):
            self.bucket_service.create(**attributes)

    def test_create_object_large_content(self):
        """Test creating an object with large content."""
        object_key = "large_object.txt"
        large_content = "A" * 10 ** 6  # 1 MB of content
        attributes = {
            'bucket_name': self.bucket_name,
            'object_key': object_key,
            'content': large_content
        }

        self.bucket_service.create(**attributes)

        # Check if the file was created physically
        full_path = os.path.join(self.test_path, self.bucket_name, object_key)
        self.assertTrue(os.path.exists(full_path))

        # Check if the content is correct
        with open(full_path, 'r') as f:
            file_content = f.read()
        self.assertEqual(file_content, large_content)
        self.bucket_service.delete(self.bucket_name, object_key)




if __name__ == '__main__':
    unittest.main()

