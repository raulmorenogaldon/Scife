"""Test file for Galgo."""
from minions.cluster.cl_minion import ClusterMinion
from storage.storage import Storage
# from os import environ as env

# import getpass
import time

# Create storage
print("Creating storage")
storage = Storage(
    path="/home/devstack/datastorage",
    public_url="161.67.100.29",
    username="devstack"
)

# Define application
# app_name = "Python test app"
# app_path = "/home/devstack/test_apps/python_test_app"
# app_creation_script = "./test.py"
# app_execution_script = "./test.py"

app_name = "CESM_1.2.2"
app_path = "/home/devstack/test_apps/cesm1_2_2"
app_creation_script = "./create_experiment.sh"
app_execution_script = "./execute_experiment.sh"

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

# Testing
print("======== Galgo =========")
cluster = ClusterMinion()
config = {
    'url': "galgo.i3a.info",
    'username': "rmoreno",
    'password': None,
    # 'password': env['GALGO_PASSWORD']
    # 'password': getpass.getpass('password: ')
}
cluster.login(config)

# Get info
images = cluster.getImages()
sizes = cluster.getFlavors()

image = images[0]
size = sizes[3]

# Create instance
instance1 = cluster.createInstance(
    "Instance1",
    image['id'],
    size['id']
)
instance2 = cluster.createInstance(
    "Instance2",
    image['id'],
    size['id']
)
instance3 = cluster.createInstance(
    "Instance3",
    image['id'],
    size['id']
)
system = {
    'master': instance1,
    'instances': [instance1, instance2, instance3]
}

# Create experiment
# labels = {
#     'DUMMY': '"Hola mundo cruel"',
#     'EXTRA': '", te odio!"',
#     'ALT': '"Lore ipsum"'
# }
labels = {
    'GRID_RESOLUTION': 'f09_g16',
    'COMPSET': 'BCN',
    'STOP_N': '365',
    'STOP_OPTION': 'ndays',
}
nodes = len(system['instances'])
experiment_id = storage.createExperiment(
    "ExperimentoLoco",
    app_id,
    nodes,
    size['cpus'],
    labels
)
experiment = storage.getExperiment(experiment_id)
print("Experiment: {0}".format(experiment_id))

# Deploy experiment in galgo
cluster.deployExperiment(app, experiment, system)

# Poll status
status = cluster.pollExperiment(experiment, system)
print("Status: {0}".format(status))
while True:
    time.sleep(1)
    status = cluster.pollExperiment(experiment, system)
    if "compiled" in status:
        print("Status changed: {0}".format(status))
        print("Compilation success!")
        break
    if "failed_compilation" in status:
        print("Status changed: {0}".format(status))
        print("Compilation FAILED!")
        exit(1)

# Execute experiment
cluster.executeExperiment(app, experiment, system)

# Poll status
status = cluster.pollExperiment(experiment, system)
print("Status: {0}".format(status))
while True:
    time.sleep(1)
    status = cluster.pollExperiment(experiment, system)
    if "done" in status:
        print("Status changed: {0}".format(status))
        print("Execution success!")
        break
    if "failed_execution" in status:
        print("Status changed: {0}".format(status))
        print("Execution FAILED!")
        exit(1)
