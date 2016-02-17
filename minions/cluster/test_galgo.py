from minions.cluster.cl_minion import ClusterMinion
from storage.storage import Storage
from os import environ as env

import getpass
import time

# Create storage
print("Creating storage")
storage = Storage(
    path="/home/devstack/datastorage",
    public_url="161.67.100.29",
    username="devstack"
)

# Define application
app_name = "Python test app"
app_path = "/home/devstack/python_test_app"
app_creation_script = "./test.py"
app_execution_script = "./test.py"

# Upload the application
print('Uploading: "{0}"'.format(app_name))
app_id = storage.createApplication(
    app_name,
    app_path,
    app_creation_script,
    app_execution_script
)

# Update app data
app = storage.getApplication(app_id)

# Create experiment
labels = {
    'DUMMY': '"Hola mundo cruel"',
    'EXTRA': '", te odio!"',
    'ALT': '"Lore ipsum"'
}
experiment_id = storage.createExperiment(
    "Experimento Loco",
    app_id,
    labels
)
experiment = storage.getExperiment(experiment_id)
print("Experiment: {0}".format(experiment_id))

# Testing
print "======== Galgo ========="
cluster = ClusterMinion()
config = {
    'url': "galgo.i3a.info",
    'username': "rmoreno",
    'password': None,
    #'password': env['GALGO_PASSWORD']
    #'password': getpass.getpass('password: ')
}
cluster.login(config)

# Get info
images = cluster.getImages()
flavors = cluster.getFlavors()

# Create instance
instance = cluster.createInstance(experiment['name'], images[0]['id'], flavors[1]['id'])

# Deploy experiment in galgo
cluster.deployExperiment(storage, app, experiment, instance)
