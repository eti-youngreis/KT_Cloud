from datetime import datetime
from help_functions import get_json
from sql_commands import insert_into_management_table, update_management_table

class Cluster:
    # BASE_PATH = "db_instances"
    def __init__(self, **kwargs):
        # self.db_instance_identifier = kwargs['db_instance_identifier']
        # self.allocated_storage = kwargs['allocated_storage']
        # self.master_user_name = kwargs['master_username']
        # self.master_user_password = kwargs['master_user_password']
        # self.db_name = kwargs.get('db_name', None)
        # self.port = kwargs.get('port', 3306)
        # self.status = 'available'
        # self.created_time = datetime.now()
        # self.endpoint = os.path.join(DBInstance.BASE_PATH, self.db_instance_identifier)
        # if not os.path.exists(self.endpoint):
        #     os.mkdir(self.endpoint)
        # self.databases = {}
        # if self.db_name:
        #     self.create_database(self.db_name)


        self.db_cluster_identifier = kwargs["db_cluster_identifier"]
        self.engine = kwargs["engine"]
        self.availability_zones = kwargs.get("availability_zones",None)
        self.copy_tags_to_snapshot = kwargs.get("copy_tags_to_snapshot",False)
        self.database_name = kwargs.get("database_name",None)
        self.db_cluster_parameter_group_name = kwargs.get("db_cluster_parameter_group_name",None)
        self.db_subnet_group_name = kwargs.get("db_subnet_group_name",None)
        self.deletion_protection = kwargs.get("deletion_protection",False)
        self.enable_cloudwatch_logs_exports = kwargs.get("enable_cloudwatch_logs_exports",None)
        self.enable_global_write_forwarding = kwargs.get("enable_global_write_forwarding",False)
        self.enable_http_endpoint = kwargs.get("enable_http_endpoint",False)
        self.enable_limitless_database = kwargs.get("",False)
        self.enable_local_write_forwarding = kwargs.get("enable_local_write_forwarding",False)
        self.engine_version = kwargs.get("engine_version",None)
        self.global_cluster_identifier = kwargs.get("global_cluster_identifier",None)
        self.option_group_name = kwargs.get("option_group_name",None)
        self.port = kwargs.get("",None)#handle defuelt values
        self.replication_source_identifier = kwargs.get("replication_source_identifier",None)
        self.scaling_configuration = kwargs.get("scaling_configuration",None)
        self.storage_encrypted = kwargs.get("storage_encrypted",None)
        self.storage_type = kwargs.get("storage_type","aurora")
        self.tags  = kwargs.get("tags",None)
        self.created_at = datetime.now()
        #create a cluster - a folder ex'....


    def get_cluster_data_in_dict(self):
        """Retrieve the metadata of the DB cluster as a JSON string."""
        data = {
            "db_cluster_identifier": self.db_cluster_identifier ,
            "engine": self.engine,
            "availability_zones": self.availability_zones ,
            "copy_tags_to_snapshot": self.copy_tags_to_snapshot ,
            "database_name": self.database_name ,
            "db_cluster_parameter_group_name": self.db_cluster_parameter_group_name ,
            "db_subnet_group_name": self.db_subnet_group_name ,
            "deletion_protection ": self.deletion_protection ,
            "enable_cloudwatch_logs_exports": self.enable_cloudwatch_logs_exports ,
            "enable_global_write_forwarding": self.enable_global_write_forwarding ,
            "enable_http_endpoint": self.enable_http_endpoint ,
            "enable_limitless_database": self.enable_limitless_database ,
            "enable_local_write_forwarding": self.enable_local_write_forwarding ,
            "engine_version": self.engine_version ,
            "global_cluster_identifier": self.global_cluster_identifier ,
            "option_group_name": self.option_group_name ,
            "port": self.port ,
            "replication_source_identifier": self.replication_source_identifier ,
            "scaling_configuration": self.scaling_configuration ,
            "storage_encrypted": self.storage_encrypted ,
            "storage_type": self.storage_type ,
            "tags": self.tags ,
            "created_at": self.created_at 
        }

        return data
    
    def save_changes_in_management_db(self, conn, exists_in_table = False):
        metadata = self.get_cluster_data_in_dict()
        del metadata['db_cluster_identifier']
        metadata_json = get_json(metadata)
        if not exists_in_table:
            insert_into_management_table(conn, "Cluster", self.db_cluster_identifier, metadata_json)
        else:
            update_management_table(conn, self.db_cluster_identifier, metadata_json)

