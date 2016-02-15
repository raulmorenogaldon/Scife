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
app_id = storage.uploadApplication(app)

# Get App data
#app = storage.getApplication(app_id)
#print(app)

# Create experiment
script = "test.py"
labels = {'DUMMY': '"Hola mundo cruel"'}

experiment_id = storage.createExperiment("Experimento Loco", app_id, script, labels)
print(experiment_id)

# List information
# listInfo()


# Testing
#print "======== Galgo ========="
#cluster = ClusterMinion()
#config = {
#    'url': "galgo.i3a.info",
#    'username': "rmoreno",
#    'password': getpass.getpass('password: ')
#}
#cluster.login(config)

# Add some flavors to the cluster
#flavor = {
#    'name': 'small',
#    'cpus': 1,
#    'ram': 1024,
#    'disk': 0
#}
#cluster.createFlavor(flavor)

def listInfo():
    images = cluster.getImages()
    print "Images:"
    i = 1
    for image in images:
        print str(i),"-->", image
        i += 1

    flavors = cluster.getFlavors()
    print "Flavors:"
    i = 1
    for flavor in flavors:
        print str(i),"-->", flavor
        i += 1
