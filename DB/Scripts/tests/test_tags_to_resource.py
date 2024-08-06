
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
from rds_client import add_tags_to_resource, list_tags_for_resource, remove_tags_from_resource, resource_object
from resource import Resource
from exception import InvalidParameterValue, ParamValidationError
from unittest.mock import Mock, PropertyMock, create_autospec
@pytest.fixture
def resource() -> Resource:
    resource:Resource = create_autospec(Resource, instance=True,spec_set=True)
    resource.add_tags = Resource.add_tags.__get__(resource, Resource)
    resource.remove_tags = Resource.remove_tags.__get__(resource, Resource)
    resource._Resource__valid_tag = Resource._Resource__valid_tag.__get__(resource, Resource) 
    type(resource).tag_list = PropertyMock(return_value={})
    resource_object['db']['my_db'] = resource
    return resource

def check_params_not_valid(err,**params):
    """general function: check raise err when params not valid"""
    with pytest.raises(err):
          add_tags_to_resource(**params)

    

def test_add_tags_to_not_exist_resource():
    tags = [{'Key':'environment','Value': 'development'}, {'Key':'project', 'Value':'my_project'}]
    resource_name = 'arn:vast:rds:eu-west-1:123456789:db:your_db'
    check_params_not_valid(InvalidParameterValue,ResourceName=resource_name,Tags=tags)

def test_raise_when_require_param_not_exist():
    tags = [{'Key':'environment','Value': 'development'}, {'Key':'project', 'Value':'my_project'}]
    resource_name = 'arn:vast:rds:eu-west-1:123456789:db:your_db'
    check_params_not_valid(ParamValidationError,Tags = tags)
    check_params_not_valid(ParamValidationError,ResourceName=resource_name)

def test_raise_when_arn_not_valid():
    tags = [{'Key':'environment','Value': 'development'}, {'Key':'project', 'Value':'my_project'}]
    resource_name1 = 'arn:vast:rds:55'
    resource_name2 = 'arn:vast:rd:55:ss:11:22'
    resource_name3 = 'arn:vast:rds:55:ss:11:22'
    check_params_not_valid(InvalidParameterValue,ResourceName=resource_name1,Tags = tags)
    check_params_not_valid(InvalidParameterValue,ResourceName=resource_name2,Tags = tags)
    check_params_not_valid(InvalidParameterValue,ResourceName=resource_name3,Tags = tags)
    
        


def test_raise_when_tag_not_valid():
    tags = 'string'
    resource_name = 'arn:aws:rds:55'
    check_params_not_valid(InvalidParameterValue,ResourceName=resource_name,Tags = tags)


def test_list_tags_for_resource_with_no_tags(resource: Resource):
    resource_name = 'arn:vast:rds:eu-west-1:123456789:db:my_db'
    result = list_tags_for_resource(ResourceName=resource_name)
    assert result == []

def test_remove_tags_not_present(resource: Resource):
    tags = [{'Key': 'environment', 'Value': 'development'}]
    resource_name = 'arn:vast:rds:eu-west-1:123456789:db:my_db'
    add_tags_to_resource(ResourceName=resource_name, Tags=tags)
    remove_tags_from_resource(ResourceName=resource_name, TagKeys=['non_existing_tag'])
    assert {'Key': 'environment', 'Value': 'development'} in list_tags_for_resource(ResourceName=resource_name)

def test_add_tags_with_invalid_keys(resource: Resource):
    tags = [{'Key': '', 'Value': 'value'}, {'Key': 'a'*129, 'Value': 'value'}]
    resource_name = 'arn:vast:rds:eu-west-1:123456789:db:my_db'
    check_params_not_valid(InvalidParameterValue, ResourceName=resource_name, Tags=tags)

def test_integration_add_remove_and_list(resource: Resource):
    tags = [{'Key': 'environment', 'Value': 'development'}, {'Key': 'project', 'Value': 'my_project'}]
    resource_name = 'arn:vast:rds:eu-west-1:123456789:db:my_db'
    
    # Add tags
    add_tags_to_resource(ResourceName=resource_name, Tags=tags)
    list_result = list_tags_for_resource(ResourceName=resource_name)
    assert all(tag in list_result for tag in tags)
    
    # Remove one tag
    remove_tags_from_resource(ResourceName=resource_name, TagKeys=['environment'])
    list_result = list_tags_for_resource(ResourceName=resource_name)
    assert {'Key': 'environment', 'Value': 'development'} not in list_result
    assert {'Key': 'project', 'Value': 'my_project'} in list_result

def test_list_tags_with_filters(resource: Resource):
    tags = [{'Key': 'environment', 'Value': 'development'}, {'Key': 'project', 'Value': 'my_project'}]
    resource_name = 'arn:vast:rds:eu-west-1:123456789:db:my_db'
    add_tags_to_resource(ResourceName=resource_name, Tags=tags)
  
    filters = [{'Name':'Key', 'Values':['environment']}]
    result = list_tags_for_resource(ResourceName=resource_name, Filters=filters)
    assert {'Key': 'environment', 'Value': 'development'} not in result
    assert {'Key': 'project', 'Value': 'my_project'} in result


        
def test_add_and_list_tags_multiple_resources():
    # Create and add a second resource
    second_resource = create_autospec(Resource, instance=True, spec_set=True)
    second_resource.add_tags = Resource.add_tags.__get__(second_resource, Resource)
    second_resource.remove_tags = Resource.remove_tags.__get__(second_resource, Resource)
    second_resource._Resource__valid_tag = Resource._Resource__valid_tag.__get__(second_resource, Resource)
    type(second_resource).tag_list = PropertyMock(return_value={})
    resource_object['db']['my_db_2'] = second_resource
    
    tags1 = [{'Key': 'environment', 'Value': 'development'}]
    tags2 = [{'Key': 'environment', 'Value': 'production'}]
    resource_name1 = 'arn:vast:rds:eu-west-1:123456789:db:my_db'
    resource_name2 = 'arn:vast:rds:eu-west-1:123456789:db:my_db_2'
    
    add_tags_to_resource(ResourceName=resource_name1, Tags=tags1)
    add_tags_to_resource(ResourceName=resource_name2, Tags=tags2)
    
    assert tags1[0] in list_tags_for_resource(ResourceName=resource_name1)
    assert tags2[0] in list_tags_for_resource(ResourceName=resource_name2)
    assert tags2[0] not in list_tags_for_resource(ResourceName=resource_name1)
