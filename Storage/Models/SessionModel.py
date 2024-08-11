import uuid
import time

class Session:
    def __init__(self, session_mode='ReadOnly', bucket=None):
        self.session_id = str(uuid.uuid4())  # Unique session identifier
        self.session_mode = session_mode      # Mode of the session (ReadOnly or ReadWrite)
        self.bucket = bucket                  # Bucket associated with the session
        self.creation_time = time.time()      # Timestamp of session creation
        self.last_access_time = self.creation_time  # Timestamp of last session access
        self.objects_accessed = set()         # Set to track accessed objects

    def is_active(self):
        """
        Check if the session is still active based on the timeout.
        
        :return: True if the session is active, False otherwise
        """
        current_time = time.time()
        return current_time - self.last_access_time < self.timeout

    def refresh(self):
        """
        Refresh the session by updating the last access time.
        """
        self.last_access_time = time.time()

    def invalidate(self):
        """
        Invalidate the session, effectively ending it.
        """
        self.session_id = None
        self.session_mode = None
        self.bucket = None
        self.objects_accessed.clear()

    def access_object(self, object_key):
        """
        Record that an object has been accessed during the session.
        
        :param object_key: The key of the object being accessed
        """
        self.objects_accessed.add(object_key)
        self.refresh()

    def get_accessed_objects(self):
        """
        Retrieve the list of objects accessed during the session.
        
        :return: A set of accessed object keys
        """
        return self.objects_accessed

    def has_access(self, required_mode='ReadOnly'):
        """
        Check if the session has the required access mode.
        
        :param required_mode: The mode required for a particular operation
        :return: True if the session mode is sufficient, False otherwise
        """
        if self.session_mode == 'ReadWrite' or (self.session_mode == 'ReadOnly' and required_mode == 'ReadOnly'):
            return True
        return False
