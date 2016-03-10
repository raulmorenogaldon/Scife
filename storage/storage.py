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

    def createApplication(self, app_cfg):
        # Get parameters
        app_name = app_cfg['name']
        app_desc = app_cfg['desc']
        app_creation_script = app_cfg['creation_script']
        app_execution_script = app_cfg['execution_script']
        app_path = app_cfg['path']

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
            self.lock = False
            raise Exception('App "{0}" already exists.'.format(
                app_name
            ))

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
            'desc': app_desc,
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

    def getApplications(self, filter=None):
        if filter is None:
            return list(self._db.applications.find())
        else:
            app = self._db.applications.find_one({'id': filter})
            if app is None:
                return list(self._db.applications.find({'name': {'$regex': '.*' + filter + '.*'}}))
            else:
                return [app]

    def createExperiment(self, exp_cfg):
        # Check parameters
        if 'name' not in exp_cfg:
            raise Exception("Error creating experiment, 'name' not set.")
        if 'app_id' not in exp_cfg:
            raise Exception("Error creating experiment, 'app_id' not set.")

        # Get parameters
        exp_name = exp_cfg['name']
        exp_app_id = exp_cfg['app_id']

        # Optionals
        if 'desc' in exp_cfg:
            exp_desc = exp_cfg['desc']
        else:
            exp_desc = "Empty"
        if 'labels' in exp_cfg:
            exp_labels = exp_cfg['labels']
        else:
            exp_labels = {}
        if 'exec_env' in exp_cfg:
            exp_exec_env = exp_cfg['exec_env']
        else:
            exp_exec_env = {}

        # Retrieve application
        app = self._findApplication(exp_app_id)
        if app is None:
            raise Exception('Application ID: "{0}", does not exists'.format(
                exp_app_id
            ))

        # Create experiment metadata
        id = str(uuid.uuid1())
        exp = {
            '_id': id,
            'id': id,
            'name': exp_name,
            'desc': exp_desc,
            'status': "created",
            'app_id': exp_app_id,
            'exec_env': exp_exec_env,
            'labels': exp_labels
        }

        # Insert into DB
        self._db.experiments.insert_one(exp)
        print('Experiment {0} created.'.format(exp['id']))
        return exp['id']

    def updateExperiment(self, exp_id, exp_cfg):
        # Retrieve experiment
        exp = self._findExperiment(exp_id)
        if exp is None:
            raise Exception('Experiment ID: "{0}", does not exists'.format(
                exp_id
            ))

        # Update
        exp = {}
        if 'name' in exp_cfg:
            exp['name'] = exp_cfg['name']
        if 'desc' in exp_cfg:
            exp['desc'] = exp_cfg['desc']
        if 'labels' in exp_cfg:
            exp['labels'] = exp_cfg['labels']
        if 'exec_env' in exp_cfg:
            exp['exec_env'] = exp_cfg['exec_env']

        # Update into DB
        self._db.experiments.update_one(
            {"id": exp_id},
            {"$set": exp}
        )

        print('Experiment {0} updated: {1}'.format(exp_id, exp))
        return exp_id

    def prepareExperiment(self, exp_id):
        # Retrieve experiment
        exp = self._findExperiment(exp_id)
        if exp is None:
            raise Exception('Experiment ID: "{0}", does not exists'.format(
                exp_id
            ))

        # Retrieve application
        app = self._findApplication(exp['app_id'])
        if app is None:
            raise Exception('Application ID: "{0}", does not exists'.format(
                exp['app_id']
            ))

        # Get application storage path
        app_path = self.path + "/" + app['id'] + "/"

        # Create experiment branch
        print('Creating experiment branch...')
        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True
        gevent.subprocess.call(["git", "checkout", "master"], cwd=app_path)
        gevent.subprocess.call(["git", "branch", "-D", exp['id']], cwd=app_path)
        gevent.subprocess.call(["git", "branch", exp['id']], cwd=app_path)

        # Apply parameters
        self._applyExperimentParams(app, exp)

        # Set public URL
        public_url = self.getExperimentPublicURL(exp_id)
        self._db.experiments.update_one(
            {"id": exp_id},
            {"$set": {"public_url": public_url}}
        )

        self.lock = False
        ########################
        return exp_id

    def getExperimentPublicURL(self, exp_id):
        # Retrieve experiment
        exp = self._findExperiment(exp_id)
        if exp is None:
            raise Exception('Experiment ID: "{0}", does not exists'.format(
                exp_id
            ))

        # Get application storage path
        app_path = self.path + "/" + exp['app_id']

        # Get public URL for this experiment
        url = "{0}@{1}:{2}".format(self.username, self.public_url, app_path)

        return url

    def getExperiments(self, filter=None):
        if filter is None:
            return list(self._db.experiments.find())
        else:
            exp = self._db.experiments.find_one({'id': filter})
            if exp is None:
                return list(self._db.experiments.find({'name': {'$regex': '.*' + filter + '.*'}}))
            else:
                return [exp]

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

        # Commit changes and return to master
        print("===============================")
        print('Committing...')
        commit_msg = "Created experiment {0}".format(experiment['id'])
        gevent.subprocess.call(["git", "add", "*"], cwd=app_path)
        gevent.subprocess.call(["git", "commit", "-m", commit_msg], cwd=app_path)
        gevent.subprocess.call(["git", "checkout", "master"], cwd=app_path)

    def _findApplication(self, app_id):
        return self._db.applications.find_one({'id': app_id})

    def _findExperiment(self, exp_id):
        return self._db.experiments.find_one({'id': exp_id})

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
