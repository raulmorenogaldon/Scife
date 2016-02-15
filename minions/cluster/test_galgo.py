from minions.cluster.cl_minion import ClusterMinion
from minions.cluster.storage import Storage
from os import environ as env

import getpass

# Create storage
print("Creating storage")
storage = Storage()

# Define application
app = {
    'name': "Python test app",
    'path': "/home/devstack/python_test_app"
}

# Upload the application
print('Uploading: "{0}"'.format(app['name']))
app_id = storage.createApplication(app)

# Update app data
app = storage.getApplication(app_id)

# Create experiment
script = "test.py"
labels = {
    'DUMMY': '"Hola mundo cruel"',
    'EXTRA': '", te odio!"',
    'ALT': '"Lore ipsum"'
}
experiment_id = storage.createExperiment("Experimento Loco", app_id, script, labels)
experiment = storage.getExperiment(experiment_id)
print("Experiment: {0}".format(experiment_id))

# Testing
print "======== Galgo ========="
cluster = ClusterMinion()
config = {
    'url': "galgo.i3a.info",
    'username': "rmoreno",
    'password': env['GALGO_PASSWORD']
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
