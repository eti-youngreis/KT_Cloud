import os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet

class EncryptionModel:
    def __init__(self) -> None:
        self.key = None
        self.nonce = None
        self.fernet_key = None

    def to_dict(self):
        return{
            "Key":self.key,
            "Nonce":self.nonce,
            "Fernet_key":self.fernet_key
        }
    
    # async def encrypt_content(self, bucket, file_data):
    #         aes_key = os.urandom(32)  
    #         nonce = os.urandom(16)  

    #         # Create a Cipher object with the key and nonce
    #         # Using CTR mode, which supports parallel encryption and decryption
    #         cipher = Cipher(algorithms.AES(aes_key), modes.CTR(nonce), backend=default_backend())
    #         encryptor = cipher.encryptor()
            
    #         # Encrypt the file_data
    #         encrypted_file_data = encryptor.update(file_data) + encryptor.finalize()
                        
    #         # Second encryption encrypt the AES key using Fernet
    #         fernet_key = Fernet.generate_key()
    #         fernet = Fernet(fernet_key)
    #         encrypted_aes_key = fernet.encrypt(aes_key)

    #         # Create metadata for the encryption configuration
    #         obj_encrypt_config = {
    #             "bucket": bucket,
    #             "key": encrypted_aes_key.hex(),
    #             "nonce": nonce.hex(),
    #             "fernet_key": fernet_key.decode()
    #         }
    #         self.key= encrypted_aes_key.hex(),
    #         self.nonce = nonce.hex(),
    #         self.fernet_key =  fernet_key.decode()

    #         return encrypted_file_data, obj_encrypt_config

    # async def decrypt_content(self, encrypted_content):
    #     # Extract the encrypted AES key and nonce from the metadata file
    #     encrypted_aes_key = bytes.fromhex(self.key)
    #     nonce = bytes.fromhex(self.nonce)
    #     fernet_key = self.fernet_key.encode()

    #     # Decrypt the AES key using Fernet
    #     fernet = Fernet(fernet_key)
    #     aes_key = fernet.decrypt(encrypted_aes_key)

    #     # Create a Cipher object with the decrypted AES key and nonce
    #     # Using CTR mode, which supports parallel encryption and decryption
    #     cipher = Cipher(algorithms.AES(aes_key), modes.CTR(nonce), backend=default_backend())
    #     decryptor = cipher.decryptor()

    #     # Decrypt the encrypted content
    #     decrypted_content = decryptor.update(encrypted_content) + decryptor.finalize()

    #     return decrypted_content
    
    # #   # Extract the encrypted AES key and nonce from the metadata file
    # #     encrypted_aes_key = bytes.fromhex(metadata_file["key"])
    # #     nonce = bytes.fromhex(metadata_file["nonce"])
    # #     fernet_key = metadata_file["fernet_key"].encode()

    # #     # Decrypt the AES key using Fernet
    # #     fernet = Fernet(fernet_key)
    # #     aes_key = fernet.decrypt(encrypted_aes_key)

    # #     # Create a Cipher object with the decrypted AES key and nonce
    # #     # Using CTR mode, which supports parallel encryption and decryption
    # #     cipher = Cipher(algorithms.AES(aes_key), modes.CTR(nonce), backend=default_backend())
    # #     decryptor = cipher.decryptor()

    # #     # Decrypt the encrypted content
    # #     decrypted_content = decryptor.update(encrypted_content) + decryptor.finalize()

    # #     return decrypted_content