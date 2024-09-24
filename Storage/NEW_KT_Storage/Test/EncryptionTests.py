import pytest
from unittest.mock import MagicMock
from cryptography.fernet import Fernet

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Service', 'Classes')))
from EncryptionService import EncryptionService

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..','..','DB','NEW_KT_DB','DataAccess')))
from DBManager import DBManager

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Exceptions')))
from EncryptionExeptions import MasterKeyNotFoundError, EncryptionError, KeyGenerationError, DatabaseError


@pytest.fixture
def mock_db_manager():
    """
    Creates a mock of DBManager suitable for all tests.
    """
    mock_db_manager = MagicMock(spec=DBManager)
    fixed_key = 'Vd1jRgmAEqZ8EmFl-8a1fy5E4Ew7Xkl-RSG_izqYB5Q='  
    mock_db_manager.execute_query_with_single_result.return_value = [fixed_key]
    return mock_db_manager

@pytest.fixture
def encryption_service(mock_db_manager):
    """
    Creates an instance of EncryptionService using a mocked DBManager.
    """
    return EncryptionService(mock_db_manager)

def test_fetch_master_key(encryption_service, mock_db_manager):
    """
    Tests that the master key is correctly retrieved from the database.
    """
    master_key = encryption_service._fetch_master_key_from_db(mock_db_manager)
    assert master_key == b'Vd1jRgmAEqZ8EmFl-8a1fy5E4Ew7Xkl-RSG_izqYB5Q='

def test_fetch_master_key_failure(mock_db_manager):
    """
    Tests the failure scenario when the master key is not found in the database.
    """
    mock_db_manager.execute_query_with_single_result.return_value = []

    with pytest.raises(DatabaseError, match="Master key not found in the database"):
        EncryptionService(mock_db_manager)


def test_generate_key(encryption_service):
    """
    Tests that the function generates a new key and encrypts it with the master key.
    """
    encrypted_key = encryption_service.generate_key()
    assert isinstance(encrypted_key, bytes)  

def test_generate_key_failer(mock_db_manager, encryption_service):
    """
    Tests handling the failure scenario when generating a new key.
    """
    encryption_service._encrypt_object_key = KeyGenerationError("Failed to generate encryption key")

    with pytest.raises(KeyGenerationError):
        encryption_service.generate_key()

def test_encrypt_success(encryption_service):
    """
    Tests successful encryption of data.
    """
    encrypted_key = encryption_service.generate_key()
    original_data = b"Secret data"
    encrypted_data = encryption_service.encrypt(original_data, encrypted_key)
    assert isinstance(encrypted_data, bytes)
    assert encrypted_data != original_data
    
def test_encrypt_invalid_key(encryption_service):
    """
    Tests scenario with an invalid key for encryption.
    """
    with pytest.raises(EncryptionError):
        encryption_service.encrypt(b"Test data", b"InvalidKey")

# Tests for decrypt

def test_decrypt_success(encryption_service):
    """
    Tests successful decryption of encrypted data.
    """
    encrypted_key = encryption_service.generate_key()
    original_data = b"Secret data"
    encrypted_data = encryption_service.encrypt(original_data, encrypted_key)
    decrypted_data = encryption_service.decrypt(encrypted_data, encrypted_key)
    assert decrypted_data == original_data

def test_decrypt_invalid_data(encryption_service):
    """
    Tests scenario with invalid data for decryption.
    """
    encrypted_key = encryption_service.generate_key()
    with pytest.raises(EncryptionError):
        encryption_service.decrypt(b"Invalid data", encrypted_key)

def test_decrypt_invalid_key(encryption_service):
    """
    Tests scenario with an incorrect key for decryption.
    """
    original_data = b"Secret data"
    encrypted_key = encryption_service.generate_key()
    encrypted_data = encryption_service.encrypt(original_data, encrypted_key)
    wrong_key = Fernet.generate_key()
    with pytest.raises(EncryptionError):
        encryption_service.decrypt(encrypted_data, wrong_key)


# Tests for _encrypt_object_key

def test_encrypt_object_key_success(encryption_service):
    """
    Tests that the key is successfully encrypted.
    """
    object_key = Fernet.generate_key()
    encrypted_object_key = encryption_service._encrypt_object_key(object_key)
    assert isinstance(encrypted_object_key, bytes)

def test_encrypt_object_key_failure(mock_db_manager):
    """
    Tests failure in encrypting the key.
    """
    mock_db_manager.execute_query_with_single_result.side_effect = Exception("Encryption error")
    with pytest.raises(DatabaseError):
        encryption_service = EncryptionService(mock_db_manager)
        encryption_service._encrypt_object_key(b"TestKey")    

# Tests for _decrypt_object_key

def test_decrypt_object_key_success(encryption_service):
    """
    Tests that the key is successfully decrypted.
    """
    object_key = Fernet.generate_key()
    encrypted_object_key = encryption_service._encrypt_object_key(object_key)
    decrypted_object_key = encryption_service._decrypt_object_key(encrypted_object_key)
    assert object_key == decrypted_object_key

def test_decrypt_object_key_failure(encryption_service):
    """
    Tests failure scenario with an incorrect key for decryption.
    """
    with pytest.raises(EncryptionError):
        encryption_service._decrypt_object_key(b"InvalidKey")
