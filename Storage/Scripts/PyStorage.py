import os
import json
from datetime import datetime
from pathlib import Path
import aiofiles

from MetaData import MetadataManager


class S3ClientSimulator:
    def __init__(self, metadata_file, server_path):
        self.metadata_manager = MetadataManager(metadata_file)
        self.server = Path(server_path)  # Ensure server_path is a Path object

    async def copy_object(self, bucket_name, copy_source, key, is_sync=True):
        source_bucket, source_key = copy_source['Bucket'], copy_source['Key']

        # Perform metadata copy
        await self.metadata_manager.copy_metadata(source_bucket, source_key, bucket_name, key, is_sync=is_sync)

        # Write the object to the filesystem
        source_file_path = self.server / source_bucket / source_key
        destination_file_path = self.server / bucket_name / key
        destination_file_path.parent.mkdir(parents=True, exist_ok=True)

        if not source_file_path.exists():
            raise FileNotFoundError(f"Source file {source_file_path} not found")

        if is_sync:
            with open(source_file_path, 'rb') as src_file:
                with open(destination_file_path, 'wb') as dest_file:
                    dest_file.write(src_file.read())
        else:
            async with aiofiles.open(source_file_path, 'rb') as src_file:
                async with aiofiles.open(destination_file_path, 'wb') as dest_file:
                    await dest_file.write(await src_file.read())

        # Get the updated metadata
        destination_metadata = self.metadata_manager.get_bucket_metadata(bucket_name, key)
        latest_version = self.metadata_manager.get_latest_version(bucket_name, key)
        destination_version_metadata = destination_metadata['versions'][latest_version]

        return {
            'CopyObjectResult': {
                'ETag': destination_version_metadata['etag'],
                'LastModified': destination_version_metadata['lastModified']
            }
        }

    async def delete_object(self, bucket_name, key, is_sync=True):
        delete_result = await self.metadata_manager.delete_object(bucket_name, key, is_sync=is_sync)
        if delete_result:
            file_path = self.server / bucket_name / key
            if file_path.exists():
                os.remove(file_path)
            return {'DeleteMarker': True}
        return {}

    async def delete_objects(self, bucket_name, delete, is_sync=True):
        deleted = []
        errors = []
        for obj in delete['Objects']:
            key = obj['Key']
            version_id = obj.get('VersionId')
            try:
                if version_id:
                    delete_result = await self.metadata_manager.delete_version(bucket_name, key, version_id,
                                                                               is_sync=is_sync)
                else:
                    delete_result = await self.metadata_manager.delete_object(bucket_name, key, is_sync=is_sync)

                if delete_result:
                    file_path = self.server / bucket_name / key
                    if file_path.exists():
                        os.remove(file_path)
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

    async def get_object_acl(self, bucket_name, key, is_sync=True):
        latest_version = self.metadata_manager.get_latest_version(bucket_name, key)
        metadata = self.metadata_manager.get_bucket_metadata(bucket_name, key)['versions'][latest_version]
        acl = metadata.get('acl', {})
        owner = acl.get('owner', 'unknown')
        permissions = acl.get('permissions', [])

        return {
            'Owner': {'DisplayName': owner, 'ID': owner},
            'Grants': [{'Grantee': {'Type': 'CanonicalUser', 'ID': owner, 'DisplayName': owner}, 'Permission': perm} for
                       perm in permissions]
        }

    async def get_object(self, bucket_name, key, is_sync=True):
        latest_version = self.metadata_manager.get_latest_version(bucket_name, key)
        metadata = self.metadata_manager.get_bucket_metadata(bucket_name, key)['versions'][latest_version]

        file_path = self.server / bucket_name / key
        if not file_path.exists():
            raise FileNotFoundError(f"Object {key} not found in bucket {bucket_name}")

        with open(file_path, 'rb') as f:
            content = f.read()

        return {
            'Body': content,
            'ContentLength': metadata.get('contentLength', len(content)),
            'ContentType': metadata.get('contentType', 'application/octet-stream'),
            'ETag': metadata['etag'],
            'Metadata': metadata.get('metadata', {}),
            'LastModified': metadata['lastModified']
        }


# Example usage
if __name__ == "__main__":
    import asyncio


    async def main():
        client = S3ClientSimulator('C:/Users/user1/Desktop/server/metadata.json', 'C:/Users/user1/Desktop/server')

        # Example for copy_object
        #copy_result = await client.copy_object('bucket2', {'Bucket': 'bucket1', 'Key': 'object1.txt'}, 'QQQQQQQ.txt',False)
        #print(copy_result)

        # Example for delete_object
        #delete_result = await client.delete_object('bucket1', 'object1.txt')
        #print(delete_result)

        # Example for delete_objects
        #delete_objects_result = await client.delete_objects('bucket2', {'Objects': [{'Key': 'kkk.txt'}]})
        #print(delete_objects_result)

        # Example for get_object_acl
        #acl_result = await client.get_object_acl('bucket1', 'object1.txt')
        #print(acl_result)

        # Example for get_object
        #object_result = await client.get_object('bucket1', 'object1.txt')
        #print(object_result)


    asyncio.run(main())
