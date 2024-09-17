from cryptography.fernet import Fernet
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..','..','DB','NEW_KT_DB','DataAccess')))
from DBManager import DBManager

class EncryptionService:
    def __init__(self, db_manager: DBManager):
        """
        Initializes the EncryptionService.
        The master key is fetched from the database and is stored privately in the instance.
        :param db_manager: An instance of DBManager to interact with the database.
        """
        try:
            # Retrieving the master key from the table
            self.__master_key = self._fetch_master_key_from_db(db_manager)
            self.__master_cipher_suite = Fernet(self.__master_key)
        except Exception as e:
            raise e

    def _fetch_master_key_from_db(self, db_manager: DBManager) -> bytes:
        """
        Fetches the master key from the database.
        :param db_manager: Instance of DBManager to query the database.
        :return: The master encryption key (in bytes) stored in the database.
        """
        try:
            query = "SELECT master_key FROM encryption_keys LIMIT 1;" 
            result = db_manager.execute_query_with_single_result(query)
            if result:
                return result[0].encode()
            else:
                raise Exception("Master key not found in the database.")
        except Exception as e:
            raise e

    def encrypt(self, data: bytes, encrypted_key: bytes) -> bytes:
        """
        Encrypts the provided data using the provided encrypted encryption key.
        :param data: Data to encrypt (in bytes).
        :param encrypted_key: Encrypted encryption key (in bytes).
        :return: Encrypted data (in bytes).
        """
        try:
            encryption_key = self._decrypt_object_key(encrypted_key)
            cipher_suite = Fernet(encryption_key)
            return cipher_suite.encrypt(data)
        except Exception as e:
            raise e

    def decrypt(self, encrypted_data: bytes, encrypted_key: bytes) -> bytes:
        """
        Decrypts the provided encrypted data using the provided encrypted encryption key.
        :param encrypted_data: Data to decrypt (in bytes).
        :param encrypted_key: Encrypted encryption key (in bytes).
        :return: Decrypted data (in bytes).
        """
        try:
            encryption_key = self._decrypt_object_key(encrypted_key)
            cipher_suite = Fernet(encryption_key)
            return cipher_suite.decrypt(encrypted_data)
        except Exception as e:
            raise e

    def generate_key(self) -> bytes:
        """
        Generates a new encryption key and returns it, encrypted with the master key.
        :return: Encrypted new encryption key (in bytes).
        """
        try:
            new_key = Fernet.generate_key()
            return self._encrypt_object_key(new_key)
        except Exception as e:
            raise e

    def _encrypt_object_key(self, object_key: bytes) -> bytes:
        """
        Encrypts the object key using the master key.
        :param object_key: The key to encrypt (in bytes).
        :return: Encrypted object key (in bytes).
        """
        try:
            return self.__master_cipher_suite.encrypt(object_key)
        except Exception as e:
            raise e

    def _decrypt_object_key(self, encrypted_object_key: bytes) -> bytes:
        """
        Decrypts the encrypted object key using the master key.
        :param encrypted_object_key: The encrypted key to decrypt (in bytes).
        :return: Decrypted object key (in bytes).
        """
        try:
            return self.__master_cipher_suite.decrypt(encrypted_object_key)
        except Exception as e:
            raise e

if __name__ == "__main__":
    
    #/////☝️Creating a table and inserting a master key for the first time!!////
    # # Create an instance of DBManager with the database file "encryption.db"
    # db_manager = DBManager("encryption.db")

    # # Create a table called 'encryption_keys' with columns 'id' as the primary key and 'master_key' as text
    # db_manager.create_table('encryption_keys', 'id INTEGER PRIMARY KEY, master_key TEXT')

    # # Import the Fernet encryption module to generate a master key
    # from cryptography.fernet import Fernet

    # # Generate a new master key and encode it as a string for storage in the database
    # master_key = Fernet.generate_key().decode()

    # # Prepare the SQL insert statement to add the master key into the 'encryption_keys' table
    # insert_query = f"INSERT INTO encryption_keys (master_key) VALUES ('{master_key}');"

    # # Execute the SQL insert statement to add the master key into the table
    # db_manager.execute_query_without_results(insert_query)

    # # Print a confirmation message that the master key has been successfully inserted
    # print(f"Master key '{master_key}' has been inserted into the encryption_keys table.")

    
    # Create an instance of DBManager (assuming your SQLite file is "encryption.db")
    db_manager = DBManager("encryption.db")

    # Create an instance of the encryption service
    encryption_service = EncryptionService(db_manager)

    # Generate an encrypted key using generate_key
    encrypted_key = encryption_service.generate_key()

    # Data to be encrypted
    data_to_encrypt = b"Hello, this is a secret message!"

    # Encrypt the data using the encrypted key
    encrypted_data = encryption_service.encrypt(data_to_encrypt, encrypted_key)
    print(f"Encrypted data: {encrypted_data}")

    # Decrypt the data back to the original text
    decrypted_data = encryption_service.decrypt(encrypted_data, encrypted_key)
    print(f"Decrypted data: {decrypted_data.decode()}")

