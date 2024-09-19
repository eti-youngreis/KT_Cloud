from flask import Flask, Config, Blueprint
from Controller import DBSubnetGroupController
from DataAccess.DBSubnetGroupManager import DBSubnetGroupManager
from Service.Classes.DBSubnetGroupService import DBSubnetGroupService
from DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from flask_injector import FlaskInjector
from injector import singleton, Binder

app = Flask(__name__)

@app.route('/')
def home():
    return 'Hello, World!'

def configure_basic_managers(binder: Binder) -> None:
    # Binding ObjectManager as singleton
    binder.bind(ObjectManager, to=ObjectManager('object_management_db.db'), scope=singleton)
    # Binding StorageManager as singleton
    binder.bind(StorageManager, to=StorageManager('DB/s3'), scope=singleton)
    
def configure_managers(binder: Binder) -> None:
    binder.bind(DBSubnetGroupManager, to=DBSubnetGroupManager, scope=singleton)
    
def configure_services(binder: Binder) -> None:
    binder.bind(DBSubnetGroupService, to=DBSubnetGroupService, scope=singleton)



if __name__ == '__main__':

    # Create a blueprint and register the controller routes
    subnet_group_blueprint = Blueprint('subnet_groups', __name__)
    DBSubnetGroupController.register_routes(subnet_group_blueprint) 
    # Register the blueprint with the Flask app
    app.register_blueprint(subnet_group_blueprint)
    FlaskInjector(app=app, modules=[configure_basic_managers, configure_managers, configure_services])
    app.run(debug=True)