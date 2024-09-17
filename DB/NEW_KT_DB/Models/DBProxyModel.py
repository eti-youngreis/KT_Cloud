from datetime import datetime
from DataAccess import ObjectManager

class DBProxy:

    def __init__(self, **kwargs) -> None:
        self.db_proxy_name = kwargs.get('db_proxy_name', None)
        #self.engine_family = kwargs.get('engine_family', None)
        self.role_arn = kwargs.get('role_arn', None)
        self.auth = kwargs.get('auth', None)
        #self.vpc_subnet_ids = kwargs.get('vpc_subnet_ids', None)
        #self.vpc_security_group_ids = kwargs.get('vpc_security_group_ids', None)
        self.require_TLS = kwargs.get('require_TLS', None) # True or False
        self.idle_client_timeout = kwargs.get('idle_client_timeout', None) # כמה זמן זה יהיה מחובר
        self.debug_logging = kwargs.get('debug_logging', None)
        self.tags = kwargs.get('tags', None)
        self.status = 'available'
        self.create_date = datetime.now()
        self.update_date = datetime.now()


        self.pk_column = 'db_proxy_name'
        self.pk_value = self.db_proxy_name


    def to_dict(self):
        return ObjectManager.convert_object_attributes_to_dictionary(
            db_proxy_name = self.db_proxy_name,
            #engine_family = self.engine_family,
            role_arn = self.role_arn,
            auth =self.auth,
            #vpc_subnet_ids = self.vpc_subnet_ids,
            #vpc_security_group_ids = self.vpc_security_group_ids,
            require_TLS = self.require_TLS,
            idle_client_timeout = self.idle_client_timeout,
            debug_logging = self.debug_logging,
            tags = self.tags,
            status = self.status,
            create_date = self.create_date,
            update_date = self.update_date,
            pk_column = self.pk_column,
            pk_value = self.pk_value
        )

        