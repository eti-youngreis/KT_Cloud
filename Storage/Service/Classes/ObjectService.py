import hashlib
import os
import sys
import asyncio
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from Models.ObjectModel import ObjectModel

from Models.Tag import Tag
from Models.AclModel import Acl

from DataAccess.ObjectManager import ObjectManager
from Service.Abc.STOE import STOE

class ObjectService(STOE):
    def __init__(self):
        self.object_manager = ObjectManager()


    async def get(self,obj:ObjectModel, version_id:str=None, flag_sync:bool=True):
        bucket = obj.bucket
        key = obj.key
        try:
            if bucket not in self.object_manager.get_buckets() or key not in self.object_manager.get_bucket(bucket)['objects'] :
                raise FileNotFoundError("bucket or key not exist")
            if version_id is None:
                version_id = self.object_manager.get_latest_version(bucket, key)
            elif version_id not in self.object_manager.get_versions(bucket, key):
                raise FileNotFoundError("version id not exist")
        
            return self.object_manager.get_object_by_version(bucket, key, version_id)
        except FileNotFoundError as e:
            print(f"Error: The file or directory was not found: {e}")
        except PermissionError as e:
            print(f"Error: Permission denied when accessing the file or directory: {e}")
        except OSError as e:
            print(f"OS error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


    async def put(self, obj: ObjectModel, body, encription=None, acl=None, metadata=None, content_type=None, flag_sync=True):

        bucket = obj.bucket
        key = obj.key
        try:
            etag = self.generate_etag(body)
            data = {
                "etag": etag,
                "size": len(body),
                "lastModified": datetime.utcnow().isoformat() + "Z",
                "isLatest": True,
                "acl": acl if acl else {"owner": "default_owner", "permissions": ["READ", "WRITE"]},
                "legalHold": {"Status": "OFF"},
                "retention": {"mode": "NONE"},
                "tagSet": [],
                "contentLength": len(body),
                "contentType": content_type if content_type else "application/octet-stream",

                "encryption": encription,
                "metadata": metadata if metadata else {}
            }
            if not self.object_manager.get_bucket(bucket):
                bucket_metadata = {'objects': {}}
                self.object_manager.metadata['server']['buckets'][bucket] = bucket_metadata
                object_metadata = {"versions": {}}
            else:
                object_metadata = self.object_manager.get_bucket(bucket)['objects'].get(key, {"versions": {}})

            # Make sure 'versions' key exists
            if 'versions' not in object_metadata:
                object_metadata['versions'] = {}

            version_id = str(len(object_metadata['versions']) + 1)
            for version in object_metadata["versions"].values():
                version["isLatest"] = False

            await self.object_manager.update()
            await self.object_manager.create(bucket, key, data, body, version_id, object_metadata, flag_sync)
        except FileNotFoundError as e:
            print(f"Error: The file or directory was not found: {e}")
            raise
        except PermissionError as e:
            print(f"Error: Permission denied when accessing the file or directory: {e}")

            raise
        except OSError as e:
            print(f"OS error occurred: {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


    def generate_etag(self, content):
            return hashlib.md5(content).hexdigest()

    async def create(self, body, acl=None, metadata=None, content_type=None, flag_sync=True):
        pass


    #naive implementation
    async def delete(self,obj:ObjectModel, version_id=None, flag_sync=True):
        bucket = obj.bucket
        key = obj.key
        version_status = self.object_manager.get_versioning_status(bucket)
        # If the versioning is not enabled, delete the object
        if version_status == 'not enabled':
            self.object_manager.delete_object(bucket,key)
            return {'DeleteMarker': False, 'VersionId': 'null'}
        # If the versionId is given or the versioning id suspended, put deleteMarker on specific version
        elif version_id or version_status == 'suspended':
            if version_status == 'suspended':
                version_id = self.object_manager.get_latest_version(bucket, key)
            await self.object_manager.put_deleteMarker(bucket, key, version_id, flag_sync=flag_sync)
            return {'DeleteMarker': True, 'VersionId': version_id}
        return {}

    
    async def put_object_tagging(self, obj: ObjectModel, tags: Tag, version_id=None, sync_flag=True):
        if not isinstance(sync_flag, bool):
            raise TypeError('sync_flag must be a boolean')
        bucket = obj.bucket
        key = obj.key
        # Check if the bucket exists
        bucket_metadata = self.object_manager.get_bucket(bucket)
        if not bucket_metadata:
            raise FileNotFoundError(f"Bucket '{bucket}' does not exist")
        # Check if the object exists
        object_metadata = self.object_manager.get_object(bucket, key)
        if object_metadata is None:
            raise FileNotFoundError(f"Object '{key}' does not exist in bucket '{bucket}'")

        # If version_id is not provided, find the latest version
        if not version_id:
            version_id = self.object_manager.get_latest_version(bucket, key)
                
        versions = self.object_manager.get_versions(bucket, key)

        if version_id not in versions:
            raise FileNotFoundError(f"Version '{version_id}' does not exist in object '{key}'")
        
        tags_dict = tags.list_tags()

        versions[version_id]["tagSet"] = tags_dict
    

        # Save metadata to file
        await self.object_manager.update(sync_flag)

        return {
            "VersionId": version_id,
            "Tag_Set": tags_dict
        }

    #naive implementation
    async def copy_object(self, source_obj:ObjectModel,destination_bucket, destination_key=None,version_id=None, sync_flag=True):
        source_bucket=source_obj.bucket
        source_key = source_obj.key
        version_status = self.object_manager.get_versioning_status(source_bucket)
        if version_status=='enabled' and version_id:
            await self.object_manager.copy_object(source_bucket, source_key, destination_bucket, destination_key,version_id,sync_flag=sync_flag)          
        await self.object_manager.copy_object(source_bucket, source_key, destination_bucket, destination_key, sync_flag=sync_flag)

    #naive implementation
    async def delete_objects(self,obj:ObjectModel, delete, flag_sync=True):
        bucket = obj.bucket
        deleted = []
        errors = []
        for object in delete['Objects']:
            version_id = object.get('VersionId',None)
            key=object['Key']
            obj = ObjectModel(bucket, object['Key'])
            
            try:
                delete_result=await self.delete(obj,version_id,flag_sync=flag_sync)
                if delete_result is not {}:
                    deleted.append({'Key': key, 'VersionId': version_id})
                else:
                    errors.append(
                        {'Key': key, 'VersionId': version_id, 'Code': 'InternalError', 'Message': 'Deletion failed'})
            except Exception as e:
                errors.append({'Key': key, 'VersionId': version_id, 'Code': 'InternalError', 'Message': str(e)})

        return {
            'Deleted': deleted,
            'Errors': errors
        }

    async def put_object_acl(self, obj: ObjectModel, acl: Acl, version_id=None, is_sync=True):
        bucket = obj.bucket
        key = obj.key
        # Check if the bucket exists
        bucket_metadata = self.object_manager.get_bucket(bucket)
        if not bucket_metadata:
            raise FileNotFoundError(f"Bucket '{bucket}' does not exist")
        # Check if the object exists
        object_metadata = self.object_manager.get_object(bucket, key)
        if object_metadata is None:
            raise FileNotFoundError(f"Object '{key}' does not exist in bucket '{bucket}'")

        # If version_id is not provided, find the latest version
        if not version_id:
            version_id = self.object_manager.get_latest_version(bucket, key)
        
        # Validate version_id
        if version_id not in self.object_manager.get_versions(bucket, key):
            raise FileNotFoundError(f"Version '{version_id}' does not exist for object '{key}' in bucket '{bucket}'")
        
        versions = self.object_manager.get_versions(bucket, key)
        # Update the Acl for the specified version
        if version_id not in versions:
            raise FileNotFoundError(f"Version '{version_id}' does not exist in object '{key}'")

        versions[version_id]["acl"] = {
            "owner": acl.owner,
            "permissions": acl.permissions
        }

        # Save metadata to file
        await self.object_manager.update(is_sync)

        return {
            "VersionId": version_id,
            "Acl": {
                "owner": acl.owner,
                "permissions": acl.permissions
            }
        }


    async def get_object_acl(self, obj: ObjectModel, version_id=None, flag_sync=True):
        try:
            bucket = obj.bucket
            key = obj.key

            bucket_metadata = self.object_manager.get_bucket(bucket)
            if not bucket_metadata:
                raise FileNotFoundError(f"Bucket '{bucket}' does not exist")

            object_metadata = self.object_manager.get_object(bucket, key)
            if object_metadata is None:
                raise FileNotFoundError(f"Object '{key}' does not exist in bucket '{bucket}'")

            versions = self.object_manager.get_versions(bucket, key)

            if version_id is None:
                version_id = self.object_manager.get_latest_version(bucket, key)

            if version_id not in versions:
                raise FileNotFoundError(f"Version '{version_id}' does not exist in object '{key}'")

            acl_metadata = versions[version_id].get("acl", {})
            owner = acl_metadata.get("owner", "unknown")
            permissions = acl_metadata.get("permissions", [])

            acl = Acl(owner=owner)
            for perm in permissions:
                acl.add_permission(perm)

            return acl
        except FileNotFoundError as e:
            raise RuntimeError(f"Metadata not found: {str(e)}")
        except KeyError as e:
            raise RuntimeError(f"Key error in metadata: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error: {str(e)}")

    async def get_object_torrent(self, version_id=None):
        pass

    async def get_object_tagging(self, obj: ObjectModel, version_id=None, sync_flag=True):
        # Validate sync_flag
        if not isinstance(sync_flag, bool):
            raise TypeError('sync_flag must be a boolean')

        bucket = obj.bucket
        key = obj.key

        # Fetch the bucket metadata
        if sync_flag:
            bucket_metadata = self.object_manager.get_bucket(bucket)
        else:
            bucket_metadata = await asyncio.to_thread(self.object_manager.get_bucket, bucket)

        if bucket_metadata is None:
            raise FileNotFoundError(f"Bucket '{bucket}' does not exist")

        # Fetch object metadata
        object_metadata = self.object_manager.get_object(bucket, key)
        if object_metadata is None:
            raise FileNotFoundError(f"Object '{key}' does not exist in bucket '{bucket}'")

        # Get the versions of the object
        versions = self.object_manager.get_versions(bucket, key)
        
        # Create a Tag object to store the tags
        tags = Tag()
        
        if version_id is not None:
            # Return tags for the specified version_id
            version_id_str = str(version_id)
            if version_id_str in versions:
                tag_set = versions[version_id_str].get('tagSet', {})
                for key, value in tag_set.items():
                    tags.add_tag(key, value)
            else:
                raise FileNotFoundError(f"Version ID '{version_id}' does not exist for object '{key}' in bucket '{bucket}'")
        else:
            # Return tags for the latest version if no version_id is provided
            latest_version_id = self.object_manager.get_latest_version(bucket, key)
            if latest_version_id is not None:
                latest_version_str = str(latest_version_id)
                tag_set = versions.get(latest_version_str, {}).get('tagSet', {})
                for key, value in tag_set.items():
                    tags.add_tag(key, value)
        
        return tags
    
    async def head_object(self, obj: ObjectModel, version_id=None, flag_async=True):
        bucket = obj.bucket
        key = obj.key

        try:
            # Verify bucket and key existence
            if bucket not in self.object_manager.get_buckets():
                raise FileNotFoundError(f"Bucket '{bucket}' does not exist.")
            bucket_metadata = self.object_manager.get_bucket(bucket)
            if key not in bucket_metadata['objects']:
                raise FileNotFoundError(f"Key '{key}' does not exist in bucket '{bucket}'.")

            # Get the object metadata
            object_metadata = self.object_manager.get_object(bucket,key)
            versions = self.object_manager.get_versions(bucket,key)
            if version_id is None:
                version_id = max(versions.keys(), key=int)
            if version_id not in versions:
                raise FileNotFoundError(f"Version '{version_id}' does not exist for key '{key}' in bucket '{bucket}'.")

            # Retrieve version metadata
            version_metadata = versions[version_id]

            # Return metadata
            return {
                'ContentLength': version_metadata.get('size', 0),
                'LastModified': version_metadata.get('lastModified', ''),
                'ContentType': version_metadata.get('contentType', 'application/octet-stream'),
                'ETag': version_metadata.get('ETag', ''),
                'Metadata': version_metadata.get('metadata', {}),
                'VersionId': version_metadata.get('VersionId', version_id),
                'ObjectLock': version_metadata.get('legalHold', {})
            }

        except FileNotFoundError as e:
            print(f"Error: {e}")
            return {'error': str(e)}
        except PermissionError as e:
            print(f"Error: Permission denied when accessing the file or directory: {e}")
            return {'error': str(e)}
        except OSError as e:
            print(f"OS error occurred: {e}")
            return {'error': str(e)}
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return {'error': str(e)}


    async def get_object_attributes(self, obj: ObjectModel, version_id=None, MaxParts=None, sync_flag=False):
        bucket = obj.bucket
        key = obj.key

        try:
            # Fetch metadata synchronously or asynchronously based on sync_flag
            if sync_flag:
                metadata = self.object_manager.get_object(bucket, key)
            else:
                metadata = await asyncio.to_thread(self.object_manager.get_object, bucket, key)

            if metadata is None:
                raise FileNotFoundError(f"No metadata found for object {key}")

            # Ensure 'versions' key exists
            versions = metadata.get('versions', {})
            if not versions:
                raise FileNotFoundError(f"No versions found for object {key}")

            # Use the latest version if no version ID is specified
            if version_id is None:
                version_id = max(versions.keys(), key=int)

            # Get version metadata
            version_metadata = versions.get(str(version_id))
            if version_metadata is None:
                raise FileNotFoundError(f"No version found with ID {version_id} for object {key}")

            # Extract and return object attributes
            attributes = {
                "checksum": version_metadata.get("checksum"),
                "ETag": version_metadata.get("ETag"),
                "ObjectParts": version_metadata.get("ObjectParts"),
                "ObjectSize": version_metadata.get("ObjectSize"),
                "StorageClass": version_metadata.get("StorageClass", {}),
            }

            # Limit ObjectParts to MaxParts if specified
            if MaxParts is not None:
                object_parts = attributes.get("ObjectParts", [])
                if object_parts:
                    attributes["ObjectParts"] = object_parts[:MaxParts]

            return attributes

        except FileNotFoundError as e:
            print(f"Error: {e}")
            return {'error': str(e)}
        except PermissionError as e:
            print(f"Error: Permission denied when accessing the file or directory: {e}")
            return {'error': str(e)}
        except OSError as e:
            print(f"OS error occurred: {e}")
            return {'error': str(e)}
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return {'error': str(e)}
        

    def list(self, *args, **kwargs):
        """list storage object."""
        pass

    def head(self, *args, **kwargs):
        """check if object exists and is accessible with the appropriate user permissions."""
        pass


    async def get_object_lock_configuration(self):
        pass

    async def put_object_legal_hold(self, legal_hold_status, version_id=None, is_sync=True):
        pass


    async def put_object_legal_hold(self, bucket:str, key:str, legal_hold_status:str, version_id:str=None, is_sync:bool=True):
        try:
            if legal_hold_status not in ["ON", "OFF"]:
                raise ValueError("Legal hold status must be either 'ON' or 'OFF'")

            if not isinstance(bucket, str) or not bucket:
                raise ValueError("Bucket name must be a non-empty string")
            if not isinstance(key, str) or not key:
                raise ValueError("Object key must be a non-empty string")
            
            metadata = self.object_manager.get_bucket_metadata(bucket, key)
            if not metadata:
                raise KeyError(f"Object key '{key}' not found in metadata")

            if "versions" not in metadata:
                metadata["versions"] = {}

            if version_id is None:
                version_id = self.object_manager.get_latest_version(bucket, key)

            if version_id not in metadata["versions"]:
                metadata["versions"][version_id] = {}

            if "LegalHold" not in metadata["versions"][version_id]:
                metadata["versions"][version_id]["LegalHold"] = {}
            metadata["versions"][version_id]["LegalHold"]["Status"] = legal_hold_status

            # Save the updated metadata based on sync/asynchronous mode
            await self.object_manager.save_metadata(is_sync)

            return {"LegalHold": {"Status": legal_hold_status}}
        

        except ValueError as e:
            return {"Error": f"Invalid value: {str(e)}"}
        except KeyError as e:
            return {"Error": f"Metadata issue: {str(e)}"}
        except Exception as e:
            return {"Error": f"Unexpected error: {str(e)}"}


    async def get_object_retention(self, version_id=None, is_sync=True):
        pass

    async def put_object_retention(self, retention_mode, retain_until_date, version_id=None, is_sync=True):
        pass

    async def put_object_lock_configuration(self, object_lock_enabled, mode="GOVERNANCE", days=30, years=0,
                                            is_sync=True):
        pass


if __name__ == '__main__':
    async def main():
        res = ObjectService()

        obj = ObjectModel("bucket3", "fff/file.txt")
        try:
            body = b"Hello, World! \n I write now I want to see if it's work"
            await res.put(obj, body)
        except Exception as e:
            print(e)
        res1 = await res.get(obj,"1")
        print(res1)

        tags = Tag()
        tags.add_tag("Project", "MyProject")
        tags.add_tag("Environment", "Production")
        await res.put_object_tagging(obj,tags,"1")
        tags=await res.get_object_tagging(obj,"1")
        print(tags)

        # Example Acl
        acl = Acl(owner="user1")
        acl.add_permission("READ")

        try:
            # Assuming the object already exists and you want to set Acl for a specific version
            response = await res.put_object_acl(obj, acl, is_sync=True)
            print(f"Acl successfully updated: {response}")
        except Exception as e:
            print(f"Error while updating Acl: {e}")

        acl = await res.get_object_acl(obj, flag_sync=True)
        print(f"Acl: {acl}")

        head_object=await res.head_object(obj)
        print(head_object,"head_object")

        atributes=await res.get_object_attributes(obj)
        print(atributes,"atributes")

    asyncio.run(main())
