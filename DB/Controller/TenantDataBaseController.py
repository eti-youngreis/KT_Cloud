from types import List
from Service import TenantDatabaseService

class TenantDataBaseController:
    def __init__(self, tenant_db_service: TenantDatabaseService):
        self.tenant_db_service = tenant_db_service
    
    def create(
        self, db_instance_identifier: str, tenant_db_name: str, master_username: str, master_user_password: str,
        character_set_name: str = None, nchar_character_set_name: str = None, tags: List[str] = [],
        cli_input_json: str = None, generate_cli_skeleton: str = None, debug: bool = False, endpoint_url: str = None,
        no_verify_ssl: bool = False, no_paginate: bool = False, output: str = None, query: str = None,
        profile: str = None, region: str = None, version: str = None, color: str = None, no_sign_request: bool = False,
        ca_bundle: str = None, cli_read_timeout: str = None, cli_connect_timeout: str = None
        ) -> None:
        return self.tenant_db_service.create(
            db_instance_identifier, tenant_db_name, master_username, master_user_password, character_set_name,
            nchar_character_set_name, tags, cli_input_json, generate_cli_skeleton, debug, endpoint_url,
            no_verify_ssl, no_paginate, output, query, profile, region, version, color, no_sign_request, ca_bundle,
            cli_read_timeout, cli_connect_timeout
        )
    
    def delete(self, db_instance_identifier: str):
        return self.tenant_db_service.delete(db_instance_identifier)
    
    def describe(self, db_instance_identifier: str):
        return self.tenant_db_service.describe(db_instance_identifier)
    
    def modify(
        self, db_instance_identifier, character_set_name: str = None, nchar_character_set_name: str = None, tags: List[str] = [],
        cli_input_json: str = None, generate_cli_skeleton: str = None, debug: bool = False, endpoint_url: str = None,
        no_verify_ssl: bool = False, no_paginate: bool = False, output: str = None, query: str = None,
        profile: str = None, region: str = None, version: str = None, color: str = None, no_sign_request: bool = False,
        ca_bundle: str = None, cli_read_timeout: str = None, cli_connect_timeout: str = None
        ):
        return self.tenant_db_service.modify(db_instance_identifier, character_set_name, nchar_character_set_name, tags,
        cli_input_json, generate_cli_skeleton, debug, endpoint_url, no_verify_ssl, no_paginate, output, query, profile,
        region, version, color, no_sign_request, ca_bundle, cli_read_timeout, cli_connect_timeout)