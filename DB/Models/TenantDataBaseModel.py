from typing import Dict, List

class TenantDatabase:
    def __init__(
        self,
        db_instance_identifier: str,
        tenant_db_name: str,
        master_username: str,
        master_user_password: str,
        character_set_name: str = None,
        nchar_character_set_name: str = None,
        tags: List[str] = [],
        cli_input_json: str = None,
        generate_cli_skeleton: str = None,
        debug: bool = False,
        endpoint_url: str = None,
        no_verify_ssl: bool = False,
        no_paginate: bool = False,
        output: str = None,
        query: str = None,
        profile: str = None,
        region: str = None,
        version: str = None,
        color: str = None,
        no_sign_request: bool = False,
        ca_bundle: str = None,
        cli_read_timeout: str = None,
        cli_connect_timeout: str = None
    ) -> None:
        self.db_instance_identifier = db_instance_identifier
        self.tenant_db_name = tenant_db_name
        self.master_username = master_username
        self.master_user_password = master_user_password
        self.character_set_name = character_set_name
        self.nchar_character_set_name = nchar_character_set_name
        self.tags = tags
        self.cli_input_json = cli_input_json
        self.generate_cli_skeleton = generate_cli_skeleton
        self.debug = debug
        self.endpoint_url = endpoint_url
        self.no_verify_ssl = no_verify_ssl
        self.no_paginate = no_paginate
        self.output = output
        self.query = query
        self.profile = profile
        self.region = region
        self.version = version
        self.color = color
        self.no_sign_request = no_sign_request
        self.ca_bundle = ca_bundle
        self.cli_read_timeout = cli_read_timeout
        self.cli_connect_timeout = cli_connect_timeout


    def to_dict(self) -> Dict:
        return {
            'db_instance_identifier': self.db_instance_identifier,
            'tenant_db_name': self.tenant_db_name,
            'master_username': self.master_username,
            'master_user_password': self.master_user_password,
            'character_set_name': self.character_set_name,
            'nchar_character_set_name': self.nchar_character_set_name,
            'tags': self.tags,
            'cli_input_json': self.cli_input_json,
            'generate_cli_skeleton': self.generate_cli_skeleton,
            'debug': self.debug,
            'endpoint_url': self.endpoint_url,
            'no_verify_ssl': self.no_verify_ssl,
            'no_paginate': self.no_paginate,
            'output': self.output,
            'query': self.query,
            'profile': self.profile,
            'region': self.region,
            'version': self.version,
            'color': self.color,
            'no_sign_request': self.no_sign_request,
            'ca_bundle': self.ca_bundle,
            'cli_read_timeout': self.cli_read_timeout,
            'cli_connect_timeout': self.cli_connect_timeout
        }