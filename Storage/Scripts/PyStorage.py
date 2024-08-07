import asyncio
import hashlib
import os
from datetime import datetime
import aiofiles
from metadata import MetadataManager

URL_SERVER = 'D:\\בוטקמפ\\server'

class S3ClientSimulator:

    def __init__(self, metadata_file=f'{URL_SERVER}/metadata.json'):
        self.metadata_manager = MetadataManager(metadata_file)

    async def get_object_attributes(self,bucket, key, version_id=None,MaxParts=None,PartNumberMarker =None,SSECustomerAlgorithm =None,SSECustomerKey =None,SSECustomerKeyMD5=None,RequestPayer=None,ExpectedBucketOwner =None,ObjectAttributes=None, sync_flag=False):
        if sync_flag:
            metadata = self.metadata_manager.get_bucket_metadata(bucket, key)
        else:
            metadata = await asyncio.to_thread(self.metadata_manager.get_bucket_metadata, bucket, key)

        if metadata is None:
            raise FileNotFoundError(f'No metadata found for object {key}')

        # Use the latest version if no version ID is specified
        if version_id is None:
            version_id = max(metadata['versions'].keys(), key=int)

        # Get version metadata
        version_metadata = metadata['versions'].get(str(version_id))
        if version_metadata is None:
            raise FileNotFoundError(f'No version found with ID {version_id} for object {key}')

        # Extract and return object attributes
        attributes = {
            'checksum': version_metadata.get('checksum'),
            'ETag': version_metadata.get('ETag'),
            'ObjectParts': version_metadata.get('ObjectParts'),
            'ObjectSize': version_metadata.get('ObjectSize'),
            'StorageClass': version_metadata.get('StorageClass', {})
        }

        # Limit ObjectParts to MaxParts if specified
        if MaxParts is not None:
            object_parts = attributes.get('ObjectParts', [])
            if object_parts:
                attributes['ObjectParts'] = object_parts[:MaxParts]

        return attributes
    async def get_object_tagging(self,bucket, key,ExpectedBucketOwner=None,RequestPayer=None, version_id=None,sync_flag=True):
        if not isinstance(sync_flag, bool):
            raise TypeError('sync_flag must be a boolean')
        # Fetch metadata for the key
        if sync_flag:
            metadata = self.metadata_manager.get_bucket_metadata(bucket, key)
        else:
            metadata = await asyncio.to_thread(self.metadata_manager.get_bucket_metadata, bucket, key)
        versions = self.metadata_manager.get_versions(bucket, key)
        if metadata is None or versions is None:
            return []
        version_id_str = str(version_id)
        if version_id_str in versions:
            return versions[version_id_str].get('TagSet', [])
        elif not version_id:
            # Get TagSet from latest version if version ID is not provided
            latest_version = self.metadata_manager.get_latest_version(bucket, key)
            if latest_version:
                return versions[str(latest_version)].get('TagSet', [])
        return []
    async def put_object_tagging(self,bucket, key,tags, version_id=None, ContentMD5=None,ChecksumAlgorithm=None,ExpectedBucketOwner=None,RequestPayer=None, sync_flag=True):
        if not isinstance(sync_flag, bool):
            raise TypeError('sync_flag must be a boolean')
        # Initialize metadata for the key if not present
        if key not in self.metadata_manager.metadata:
            self.metadata_manager.metadata[key] = {'versions': {}}
        versions = self.metadata_manager.get_versions(bucket, key)
        version_id_str = str(version_id)
        if version_id_str in versions:
            # Update TagSet for existing version
            versions[version_id_str]['TagSet'] = tags['TagSet']
        elif version_id:
            # Add new version with tags
            versions[version_id_str] = tags
        else:
            latest_version = self.metadata_manager.get_latest_version(bucket,key)
            if latest_version:
                versions[str(latest_version)]['TagSet'] = tags['TagSet']
            else:
                versions['0'] = tags
        # Save metadata
        if sync_flag:
            await self.metadata_manager.save_metadata(True)
        else:
            await self.metadata_manager.save_metadata(False)
    async def get_object_lock_configuration(self, bucket):
        # Check if the bucket exists
        bucket_metadata = self.metadata_manager.metadata['server']['buckets'].get(bucket)

        if bucket_metadata is None:
            raise FileNotFoundError(f'Bucket {bucket} not found.')

        # Retrieve the object lock configuration for the bucket
        object_lock = bucket_metadata.get('objectLock', None)
        object_lock_configuration = {
            'ObjectLockEnabled': object_lock.get('objectLockEnabled', 'DISABLED'),
            'LockConfiguration': object_lock['lockConfiguration'] if object_lock else {}
        }

        return {
            'ObjectLockConfiguration': object_lock_configuration
        }


    async def get_object_torrent(self, bucket, key, version_id=None, sync_flag=True, IfMatch=None,if_modified_since=None,if_none_match=None,
        if_unmodified_since=None,range=None,ssec_ustomer_algorithm=None,ssec_ustomer_key=None,ssec_ustomerkey_md5=None,request_payer=None,):

        # Retrieve the object metadata
        metadata = self.metadata_manager.get_bucket_metadata(bucket, key)

        if not metadata:
            raise FileNotFoundError(f"Object {key} not found in bucket {bucket}")
            raise FileNotFoundError(f'Object {key} not found in bucket {bucket}')

        # If version_id is provided, fetch that specific version
        if version_id:
            version_metadata = metadata.get('versions', {}).get(version_id)
            if not version_metadata:
                raise FileNotFoundError(f"Version {version_id} not found for object {key} in bucket {bucket}")
                raise FileNotFoundError(f'Version {version_id} not found for object {key} in bucket {bucket}')
        else:
            # If no version_id is provided, get the latest version
            version_id = self.metadata_manager.get_latest_version(bucket, key)
            version_metadata = metadata.get('versions', {}).get(version_id)

        # Prepare the torrent information (this is a placeholder, modify as needed)
        torrent_info = {
            'bucket': bucket,
            'key': key,
            'version_id': version_id,
            'etag': version_metadata.get('etag'),
            'size': version_metadata.get('size'),
            'last_modified': version_metadata.get('lastModified'),
            'content_type': version_metadata.get('contentType'),
            'metadata': version_metadata.get('metadata', {})
        }

        return torrent_info

    async def head_object(self, bucket, key, version_id=None,is_async=True,IfMatch=None, IfModifiedSince=None,IfNoneMatch=None,IfUnmodifiedSince=None,
    Range=None,VersionId=None,SSECustomerAlgorithm=None,SSECustomerKey=None,SSECustomerKeyMD5=None,RequestPayer=None):

        # Retrieve the object metadata
        metadata = self.metadata_manager.get_bucket_metadata(bucket, key)

        if not metadata:
            raise FileNotFoundError(f'Object {key} not found in bucket {bucket}')
            
        # If version_id is provided, fetch that specific version
        if version_id:
            version_metadata = metadata.get('versions', {}).get(version_id)
            if not version_metadata:
                raise FileNotFoundError(f'Version {version_id} not found for object {key} in bucket {bucket}')
        else:
            # If no version_id is provided, get the latest version
            version_id = self.metadata_manager.get_latest_version(bucket, key)
            version_metadata = metadata.get('versions', {}).get(version_id)
        if not version_metadata:
            raise FileNotFoundError(f'No version metadata found for object {key} with version {version_id}')

        # Prepare the response metadata
        response_metadata = {
            'ContentLength': version_metadata.get('size'),
            'LastModified': version_metadata.get('lastModified'),
            'ContentType': version_metadata.get('contentType'),
            'ETag': version_metadata.get('etag'),
            'Metadata': version_metadata.get('metadata', {}),
            'VersionId': version_id,
            'ObjectLock': metadata.get('objectLock', {})
        }

        return response_metadata
    async def put_object(self, bucket, key, body, acl=None, metadata=None,

    content_type=None, sse_customer_algorithm=None,sse_customer_key=None, sse_customer_key_md5=None,sync_flag=True):

        # Check if the bucket exists
        bucket_metadata = self.metadata_manager.get_metadata(bucket)

        if not bucket_metadata:
            bucket_metadata = {'objects': {}}
            self.metadata_manager.metadata['server']['buckets'][bucket] = bucket_metadata

        object_metadata = bucket_metadata['objects'].get(key, {'versions': {}})

        # Determine the new version ID
        version_id = str(len(object_metadata['versions']) + 1)  # Simple versioning

        # Create the file path with the new version ID before the extension
        file_name, file_extension = os.path.splitext(key)
        versioned_file_name = f'{file_name}.v{version_id}{file_extension}'
        file_path = os.path.join(URL_SERVER, bucket, versioned_file_name)

        # Create the directory structure if it doesn't exist
        if '/' in key:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Write the body to the file
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(body)

        # Update previous versions' isLatest to False
        for version in object_metadata["versions"].values():
            version["isLatest"] = False

        # Prepare the metadata for the new version
        etag = self.generate_etag(body)
        object_metadata["versions"][version_id] = {
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
            "metadata": metadata if metadata else {}
        }

        bucket_metadata["objects"][key] = object_metadata

        # Save metadata to file
        await self.metadata_manager.save_metadata()

        return {
            "ETag": object_metadata["versions"][version_id]["etag"],
            "VersionId": version_id
        }
    async def put_object_acl(self, bucket, key, acl, version_id=None,is_sync=True,GrantFullControl=None,GrantRead=None,GrantReadACP=None,GrantWriteACP=None):
        # Check if the bucket exists
        bucket_metadata = self.metadata_manager.metadata["server"]["buckets"].get(bucket)
        if not bucket_metadata:
            raise FileNotFoundError(f"Bucket '{bucket}' does not exist")

        object_metadata = bucket_metadata["objects"].get(key)

        if not object_metadata:
            raise FileNotFoundError(f"Object '{key}' does not exist in bucket '{bucket}'")

        # If version_id is not provided, find the latest version
        if not version_id:
            for vid, metadata in object_metadata["versions"].items():
                if metadata["isLatest"]:
                    version_id = vid
                    break

        if not version_id or version_id not in object_metadata["versions"]:
            raise FileNotFoundError(f"Version '{version_id}' does not exist for object '{key}' in bucket '{bucket}'")

        # Update the ACL for the specified version
        object_metadata["versions"][version_id]["acl"] = acl

        # Save metadata to file
        await self.metadata_manager.save_metadata(is_sync)

        return {
            "VersionId": version_id,
            "ACL": acl
      
        }
    def generate_etag(self, content):
        return hashlib.md5(content).hexdigest()

async def main():
    s3_client = S3ClientSimulator(f'{URL_SERVER}\\metadata.json')
    try:
        lock_config = await s3_client.get_object_lock_configuration('bucket1')
        print(lock_config)
    except FileNotFoundError as e:
        print(e)
    try:
        torrent_info = await s3_client.get_object_torrent('bucket1', 'file.txt')
        print('Torrent Info for latest version:', torrent_info)
    except FileNotFoundError as e:
        print(e)

    # Example of getting specific version torrent info
    try:
        torrent_info = await s3_client.get_object_torrent('bucket1', 'file.txt', version_id='1')
        print('Torrent Info for version 1:', torrent_info)
    except FileNotFoundError as e:
        print(e)
    head_object = await s3_client.head_object('bucket1', 'file2.txt')
    print('meta data is:', head_object)
    try:
        # Example body as bytes
        body = b'Hello, World!'
        result = await s3_client.put_object('bucket2', 'new-file.txt', body)
        print('PutObject result:', result)
    except Exception as e:
        print(e)
    try:
        response = await s3_client.put_object_acl('bucket1', 'file2.txt', {'owner': 'default_owner', 'permissions': ['READ', 'WRITE']},version_id='3')
        print(response)
    except Exception as e:
        print(e)

if __name__ == '__main__':
    asyncio.run(main())

