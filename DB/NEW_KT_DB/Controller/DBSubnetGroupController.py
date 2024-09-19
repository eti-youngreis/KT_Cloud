from Service.Classes.DBSubnetGroupService import DBSubnetGroupService
from flask.views import View
from injector import inject
from flask import Blueprint, request, jsonify
  
def register_routes(blueprint: Blueprint):
    @blueprint.route('/subnet-group', methods=['POST'])
    def create_db_subnet_group(service: DBSubnetGroupService):
        data = request.get_json()
        try:
            service.create_db_subnet_group(**data)
            return jsonify({"message": "Subnet group created successfully!"}), 201
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @blueprint.route('/subnet-group/<name>', methods=['DELETE'])
    def delete_subnet_group(name, service: DBSubnetGroupService):
        try:
            service.delete_db_subnet_group(name)
            return jsonify({"message": "Subnet group deleted successfully!"})
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @blueprint.route('/subnet-group/<name>', methods=['PUT'])
    def modify_subnet_group(name, service: DBSubnetGroupService):
        updates = request.get_json()
        try:
            service.modify_db_subnet_group(name, **updates)
            return jsonify({"message": "Subnet group modified successfully!"})
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @blueprint.route('/subnet-group/<name>', methods=['GET'])
    def get_subnet_group(name, service: DBSubnetGroupService):
        try:
            # describe and not get because this is an api interface
            subnet_group = service.describe_db_subnet_group(name)
            print(subnet_group)
            return jsonify(subnet_group)
        except ValueError as e:
            return jsonify({"error": str(e)}), 404

    @blueprint.route('/subnet-groups', methods=['GET'])
    def list_subnet_groups(service: DBSubnetGroupService):
        try:
            subnet_groups = service.describe_subnet_groups()
            return jsonify(subnet_groups)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
