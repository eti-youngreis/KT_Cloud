import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from Models.ObjectModel import ObjectModel
from Models.Tag import Tag
from Models.AclModel import Acl
from Service.Classes.ObjectService import ObjectService
from Validation.Validation import validate_required_params


class ObjectController:
    def __init__(self):
        self.object_service = ObjectService()

    async def get_object(
        self, user_name: str, bucket: str, key: str, version_id: str = None
    ) -> ObjectModel:
        """Retrieve an object."""
        validate_required_params(user_name=user_name, bucket=bucket, key=key)
        return await self.object_service.get_object(
            user_name=user_name, bucket=bucket, key=key, version_id=version_id
        )

    async def put_object(
        self,
        user_name: str,
        bucket: str,
        key: str,
        body: bytes,
        encryption: str = None,
        acl: str = None,
        metadata: dict = None,
        content_type: str = None,
    ):
        """Upload an object."""
        validate_required_params(user_name=user_name, bucket=bucket, key=key, body=body)
        return await self.object_service.put_object(
            user_name=user_name,
            bucket=bucket,
            key=key,
            body=body,
            encription=encryption,
            acl=acl,
            metadata=metadata,
            content_type=content_type,
        )

    async def delete_object(
        self, user_name: str, bucket: str, key: str, version_id: str = None
    ):
        """Delete an object."""
        validate_required_params(user_name=user_name, bucket=bucket, key=key)
        return await self.object_service.delete_object(
            user_name=user_name, bucket=bucket, key=key, version_id=version_id
        )

    async def delete_objects(self, user_name: str, bucket: str, delete):
        """Delete an object."""
        validate_required_params(user_name=user_name, bucket=bucket, delete=delete)
        user_name, bucket, key = ObjectModel(
            bucket,
        )
        return await self.object_service.delete_object(
            user_name=user_name, bucket=bucket, delete=delete
        )

    async def copy_object(
        self,
        user_name: str,
        bucket,
        key,
        destination_bucket,
        destination_key=None,
        version_id=None,
        sync_flag=True,
    ):
        validate_required_params(
            user_name=user_name,
            bucket=bucket,
            key=key,
            destination_bucket=destination_bucket,
        )
        return await self.object_service.copy_object(
            user_name,
            bucket,
            key,
            destination_bucket,
            destination_key=None,
            version_id=None,
            sync_flag=True,
        )

    async def put_object_tagging(
        self, user_name: str, bucket: str, key: str, tags: dict, version_id: str = None
    ):
        """Add or update object tags."""
        validate_required_params(user_name=user_name, bucket=bucket, key=key, tags=tags)
        tag_obj = Tag()
        for tag_key, tag_value in tags.items():
            tag_obj.add_tag(tag_key, tag_value)
        return await self.object_service.put_object_tagging(
            user_name, bucket, key, tags, version_id
        )

    async def get_object_tagging(
        self, user_name: str, bucket: str, key: str, version_id: str = None
    ):
        """Retrieve object tags."""
        validate_required_params(user_name=user_name, bucket=bucket, key=key)
        return await self.object_service.get_object_tagging(
            user_name, bucket, key, version_id
        )

    async def put_object_acl(
        self,
        user_name: str,
        bucket: str,
        key: str,
        owner: str,
        permissions: list,
        version_id: str = None,
    ):
        """Set object access control list (ACL)."""
        validate_required_params(
            user_name=user_name,
            bucket=bucket,
            key=key,
            owner=owner,
            permissions=permissions,
        )
        acl = Acl(owner=owner)
        for perm in permissions:
            acl.add_permission(perm)
        return await self.object_service.put_object_acl(
            user_name, bucket, key, acl, version_id
        )

    async def get_object_acl(
        self, user_name: str, bucket: str, key: str, version_id: str = None
    ):
        """Retrieve object access control list (ACL)."""
        validate_required_params(user_name=user_name, bucket=bucket, key=key)
        return await self.object_service.get_object_acl(
            user_name, bucket, key, version_id
        )

    async def head_object(
        self, user_name: str, bucket: str, key: str, version_id: str = None
    ):
        """Retrieve object metadata (without the actual content)."""
        validate_required_params(user_name=user_name, bucket=bucket, key=key)
        return await self.object_service.head_object(user_name, bucket, key, version_id)

    async def get_object_attributes(
        self, user_name: str, bucket: str, key: str, version_id: str = None
    ):
        """Retrieve specific attributes of an object."""
        validate_required_params(user_name=user_name, bucket=bucket, key=key)
        return await self.object_service.get_object_attributes(
            user_name, bucket, key, version_id
        )

    def list(self, *args, **kwargs):
        """List storage objects."""
        pass

    def head(self, *args, **kwargs):
        """Check if object exists and is accessible with the appropriate user permissions."""
        pass

    async def put_object_legal_hold(
        self,
        user_name: str,
        bucket: str,
        key: str,
        legal_hold_status: str,
        version_id: str = None,
        is_sync: bool = True,
    ):
        """Set or update the legal hold status of an object."""
        pass

    async def put_object_retention(
        self, retention_mode, retain_until_date, version_id=None, is_sync=True
    ):
        """Set or update the retention settings of an object."""
        pass

    async def get_object_retention(self, version_id=None, is_sync=True):
        """Retrieve the retention settings of an object."""
        pass

    async def put_object_lock_configuration(
        self, object_lock_enabled, mode="GOVERNANCE", days=30, years=0, is_sync=True
    ):
        """Set or update the object lock configuration."""
        pass

    async def get_object_lock_configuration(self):
        """Retrieve the object lock configuration."""
        pass
