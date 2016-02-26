import json
import getopt
import gevent.subprocess
import os
import re
import sys
import uuid
import zerorpc

import pymongo
from pymongo import MongoClient

class Storage(object):
    """Class to handle application storage in standard a FS."""

    def __init__(self, path, public_url, username):
        print("Initializing storage...")

        # Set path for storage
        self.path = path

        # Set user
        self.username = username

        # Set public url
        self.public_url = public_url

        # Connect to DB (default "localhost")
        # db vars must be private to avoid zerorpc errors
        print("Connecting to DB...")
        try:
            self._db_client = MongoClient()
            self._db = self._db_client.test_db
        except Exception as e:
            print("Failed to get Storage database, reason: ", e)
            raise e

        try:
            self._db.applications.create_index([
                ('id', pymongo.ASCENDING),
                ('name', pymongo.ASCENDING)
            ], unique=True)
            self._db.experiments.create_index([
                ('id', pymongo.ASCENDING),
                ('name', pymongo.ASCENDING)
            ], unique=True)
        except Exception as e:
            print("Failed to create DB index, reason: ", e)
            raise e

        # Init lock
        self.lock = False

        # Check if datastorage exists
        if not os.path.isdir(self.path):
            # Create storage folder
            os.mkdir(self.path)

        # Load apps
        print("Loading apps in DB...")
        cursor = self._db.applications.find()
        if cursor.count() == 0:
            print("No apps in DB!")
        else:
            for app in cursor:
                # Check if exists in folder
                app_path = os.path.join(self.path, app['id'])
                if os.path.isdir(app_path):
                    print("App loaded:", app['name'], app['id'])
                else:
                    print("App {0} - {1} not found!, removing from DB...".format(
                        app['name'], app['id']
                    ))
                    self._db.applications.delete_one({'id': app['id']})
        print("Initialization completed!")

    def createApplication(self, app_name, app_path, app_creation_script, app_execution_script):
        # Check if config is valid
        if not(os.path.isdir(app_path)):
            raise IOError("Invalid input path, does not exists: {0}".format(
                app_path
            ))

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Check if application name exists
        cursor = self._db.applications.find({'name': app_name})
        for app in cursor:
            print('App "{0}" already exists.'.format(app_name))
            self.lock = False
            return app['id']

        # Create UUID for application
        id = str(uuid.uuid1())

        # Get source path
        src_path = app_path

        # Get destination path and create it
        dst_path = self.path + "/" + id

        # Copy application to storage
        print('Copying app "{0}": {1} --> {2}'.format(
            app_path, src_path, dst_path
        ))
        gevent.subprocess.call(["scp", "-r", src_path, dst_path])

        # Create application data
        app = {
            '_id': id,
            'id': id,
            'name': app_name,
            'desc': "Description...",
            'creation_script': app_creation_script,
            'execution_script': app_execution_script,
        }

        # Create labels list
        app['labels'] = []
        print('Discovering parameters...')
        for file in os.listdir(dst_path):
            file = os.path.join(dst_path, file)
            if os.path.isfile(file):
                app['labels'] = list(set(app['labels'] + self._getLabelsInFile(file)))

        # Create application json
        file_path = "{0}/app.json".format(dst_path)
        with open(file_path, "w") as file:
            json.dump(app, file)

        # Create git repository for this app
        print('Creating repository...')
        gevent.subprocess.call(["git", "init"], cwd=dst_path)
        gevent.subprocess.call(["git", "add", "*"], cwd=dst_path)
        gevent.subprocess.call(["git", "commit", "-q", "-m", "'Application created'"], cwd=dst_path)

        # Add application to DB
        self._db.applications.insert_one(app)

        self.lock = False
        ########################

        return app['id']

    def _getLabelsInFile(self, file):
        print("Getting labels from file: {0}".format(file))
        # Load file
        f = open(file, 'r')
        filedata = f.read()
        f.close()
        # Find labels
        labels = re.findall(r"\[\[\[(\w+)\]\]\]", filedata)
        labels = list(set(labels))
        print("Found: {0}".format(labels))
        return labels

    def findApplication(self, app_id):
        for app in self._db.applications.find({'id': app_id}):
            return app
        return None

    def getApplications(self, filter=""):
        return list(self._db.applications.find())

    def createExperiment(self, name, app_id, exec_env, labels):
        # Retrieve application
        app = self.findApplication(app_id)
        if app is None:
            raise Exception("Application ID does not exists")

        # Create experiment metadata
        id = str(uuid.uuid1())
        experiment = {
            '_id': id,
            'id': id,
            'name': name,
            'desc': "Description...",
            'app_id': app['id'],
            'exec_env': exec_env,
            'labels': labels
        }

        # Get application storage path
        app_path = self.path + "/" + app['id'] + "/"

        # Create experiment branch
        print('Creating experiment branch...')
        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True
        gevent.subprocess.call(["git", "branch", experiment['id']], cwd=app_path)

        # Apply parameters
        self._applyExperimentParams(app, experiment)

        # Set public URL
        experiment['public_url'] = self.getExperimentPublicURL(experiment)

        self._db.experiments.insert_one(experiment)
        self.lock = False
        ########################
        print('Experiment {0} created.'.format(experiment['id']))
        return experiment['id']

    def getExperimentPublicURL(self, experiment):
        # Get application storage path
        app_path = self.path + "/" + experiment['app_id']

        # Get public URL for this experiment
        url = "{0}@{1}:{2}".format(self.username, self.public_url, app_path)

        return url

    def findExperiment(self, experiment_id):
        # Search experiment
        for exp in self._db.experiments.find({'id': experiment_id}):
            return exp
        return None

    def getExperiments(self, filter=""):
        return list(self._db.experiments.find())

    def _replaceLabelsInFile(self, file, labels):
        print("Replacing labels in file: {0}".format(file))
        # Load file
        f = open(file, 'r')
        filedata = f.read()
        f.close()
        # Replace labels
        for label in labels.keys():
            key = "[[[{0}]]]".format(label)
            value = labels[label]
            filedata = filedata.replace(key, str(value))
        # Write file
        f = open(file, 'w')
        f.write(filedata)
        f.close()

    def _applyExperimentParams(self, app, experiment):
        # Get application storage path
        app_path = self.path + "/" + app['id'] + "/"

        # Check out experiment
        print("===============================")
        print('Checking out experiment branch...')
        gevent.subprocess.call(["git", "checkout", experiment['id']], cwd=app_path)

        # Get execution environment
        exec_env = experiment['exec_env']

        # Get labels and add system ones
        labels = experiment['labels']
        labels['#EXPERIMENT_ID'] = experiment['id']
        labels['#EXPERIMENT_NAME'] = experiment['name']
        labels['#APPLICATION_ID'] = app['id']
        labels['#APPLICATION_NAME'] = app['name']
        labels['#INPUTPATH'] = exec_env['inputpath']
        labels['#LIBPATH'] = exec_env['libpath']
        labels['#TMPPATH'] = exec_env['tmppath']
        labels['#CPUS'] = str(exec_env['cpus'])
        labels['#NODES'] = str(exec_env['nodes'])
        labels['#TOTALCPUS'] = str(exec_env['nodes'] * exec_env['cpus'])

        # List labels
        for label in labels.keys():
            key = "[[[{0}]]]".format(label)
            value = labels[label]
            print("Replacing: {0} <-- {1}".format(key, value))

        # List of empty labels
        empty_labels = {}
        for label in app['labels']:
            empty_labels[label] = ""

        # Apply labels
        print("===============================")
        print("Applying labels...")
        for file in os.listdir(app_path):
            file = os.path.join(app_path, file)
            if os.path.isfile(file):
                self._replaceLabelsInFile(file, labels)
                self._replaceLabelsInFile(file, empty_labels)

        # Execute experiment creation script
        #if app['creation_script'] is not None:
        #    print("===============================")
        #    comm = "./{0}".format(app['creation_script'])
        #    print("Executing experiment creation script: {0}".format(comm))
        #    subprocess.call([comm], cwd=app_path)

        # Commit changes and return to master
        print("===============================")
        print('Committing...')
        commit_msg = "Created experiment {0}".format(experiment['id'])
        gevent.subprocess.call(["git", "add", "*"], cwd=app_path)
        gevent.subprocess.call(["git", "commit", "-m", commit_msg], cwd=app_path)
        gevent.subprocess.call(["git", "checkout", "master"], cwd=app_path)


# Start RPC server
# Execute this only if called directly from python command
# From now RPC is waiting for requests
if __name__ == "__main__":
    # Get arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:", ["cfg="])
        if len(opts) == 0:
            raise getopt.GetoptError("Please, specify configuration file with -c")
    except getopt.GetoptError as e:
        # Not valid arguments
        print(e)
        print("Usage: python -m storage.storage -c <config_file>")
        sys.exit(2)

    # Iterate arguments
    for opt, arg in opts:
        if opt == '-h':
            print("python -m storage.storage -c <config_file>")
            sys.exit()
        elif opt in ("-c", "--cfg"):
            cfg_file = arg

    print("Config file: {0}".format(cfg_file))

    # Check if config file exists
    if os.path.isfile(cfg_file):
        with open(cfg_file) as f:
            cfg = json.load(f)
    else:
        print("Inexistent file: {0}".format(cfg_file))
        sys.exit(3)

    # Read configuration file
    rpc = zerorpc.Server(Storage(
        path=cfg['path'],
        public_url=cfg['public_url'],
        username=cfg['username']
    ), heartbeat=30)
    rpc.bind("tcp://0.0.0.0:8237")
    rpc.run()
