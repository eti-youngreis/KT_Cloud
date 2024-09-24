import os
import sys
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from DB.NEW_KT_DB.Validation.GeneralValidations import is_valid_number, is_length_in_range

def validate_allocated_storage(allocated_storage):
    """Validate the allocated_storage value."""
    if not is_valid_number(allocated_storage, 1, 10000):
        raise ValueError(f"Invalid allocated_storage: {allocated_storage}. Must be between 1 and 10000.")

def validate_master_user_name(name):
    """Validate the master_user_name value."""
    if not is_length_in_range(name,0,50):
        raise ValueError("master_user_name must be at least 5 characters long.")

def validate_master_user_password(password):
    """Validate the master_user_password value."""
    if not is_length_in_range(password,0, 80):
        raise ValueError("master_user_password must be at least 8 characters long.")

def validate_port(port):
    """Validate the port value."""
    if not is_valid_number(port, 1024, 65535):
        raise ValueError(f"Invalid port: {port}. Must be between 1024 and 65535.")

def validate_status(status):
    """Validate the status value."""
    valid_statuses = ['available', 'modifying', 'deleting', 'backing-up', 'stopped']
    if status not in valid_statuses:
        raise ValueError(f"Invalid status: {status}. Must be one of {valid_statuses}.")
