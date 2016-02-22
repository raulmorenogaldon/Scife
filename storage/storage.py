import json
import os
import re
import shutil
import subprocess
import uuid
import zerorpc


class Storage(object):
    """Class to handle application storage in standard a FS."""

    def __init__(self, path, public_url, username):
        print("Initializing storage...")
        # Create applications and experiments array
        self.applications = []
        self.experiments = []

        # Set path for storage
        self.path = path

        # Set user
        self.username = username

        # Set public url
        self.public_url = public_url

        # Check if datastorage exists
        if os.path.isdir(self.path):
            print("Loading apps in storage folder...")
            # Load existings applications
            self._loadApplications(self.path)

        else:
            # Create storage folder
            os.mkdir(self.path)
        print("Initialization completed!")

    def _loadApplications(self, folder):
        # Iterate folders
        for dir in os.listdir(folder):
            dir = os.path.join(folder, dir)
            if os.path.isdir(dir):
                # Load application json
                file = "{0}/app.json".format(dir)
                if os.path.exists(file):
                    f = open(file, "r")
                    app = json.loads(f.read())
                    f.close()
                    print("Loaded app: {0} - {1}".format(
                        app['name'], app['id']
                    ))
                    self.applications.append(app)

    def createApplication(self, app_name, app_path, app_creation_script, app_execution_script):
        # Check if config is valid
        if not(os.path.isdir(app_path)):
            raise IOError("Invalid input path, does not exists: {0}".format(
                app_path
            ))

        # Check if application name exists
        for app in self.applications:
            if app['name'] == app_name:
                print('App "{0}" already exists.'.format(app_name))
                return app['id']

        # Create UUID for application
        id = str(uuid.uuid1())

        # Get source path
        src_path = app_path

        # Get destination path and create it
        dst_path = self.path + "/" + id + "/"

        # Copy application to storage
        print('Copying app "{0}": {1} --> {2}'.format(
            app_path, src_path, dst_path
        ))
        shutil.copytree(src_path, dst_path)

        # Create application data
        app = {
            'id': id,
            'name': app_name,
            'desc': "Description...",
            'creation_script': app_creation_script,
            'execution_script': app_execution_script,
        }

        # Create labels list
        print('Discovering parameters...')
        for file in os.listdir(dst_path):
            file = os.path.join(dst_path, file)
            if os.path.isfile(file):
                app['labels'] = self._getLabelsInFile(file)

        # Add application to DB
        self.applications.append(app)

        # Create application json
        file_path = "{0}/app.json".format(dst_path)
        with open(file_path, "w") as file:
            json.dump(app, file)

        # Create git repository for this app
        print('Creating repository...')
        subprocess.call(["git", "init"], cwd=dst_path)
        subprocess.call(["git", "add", "*"], cwd=dst_path)
        subprocess.call(["git", "commit", "-q", "-m", "'Application created'"], cwd=dst_path)

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

    def getApplication(self, app_id):
        # Search app
        for app in self.applications:
            if app['id'] == app_id:
                return app
        return None

    def createExperiment(self, name, app_id, nodes, cpus, labels):
        # Retrieve application
        app = self.getApplication(app_id)
        if app is None:
            raise Exception("Application ID does not exists")

        # Create experiment metadata
        experiment = {
            'id': str(uuid.uuid1()),
            'name': name,
            'desc': "Description...",
            'app_id': app['id'],
            'nodes': nodes,
            'cpus': cpus,
            'labels': labels
        }

        # Get application storage path
        app_path = self.path + "/" + app['id'] + "/"

        # Create experiment branch
        print('Creating experiment branch...')
        subprocess.call(["git", "branch", experiment['id']], cwd=app_path)

        # Apply parameters
        self._applyExperimentParams(app, experiment, nodes, cpus)

        # Set public URL
        experiment['public_url'] = self.getExperimentPublicURL(experiment)

        self.experiments.append(experiment)
        return experiment['id']

    def getExperimentPublicURL(self, experiment):
        # Get application storage path
        app_path = self.path + "/" + experiment['app_id']

        # Get public URL for this experiment
        url = "{0}@{1}:{2}".format(self.username, self.public_url, app_path)

        return url

    def getExperiment(self, experiment_id):
        # Search experiment
        for exp in self.experiments:
            if exp['id'] == experiment_id:
                return exp
        return None

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
            filedata = filedata.replace(key, value)
        # Write file
        f = open(file, 'w')
        f.write(filedata)
        f.close()

    def _applyExperimentParams(self, app, experiment, nodes, cpus):
        # Get application storage path
        app_path = self.path + "/" + app['id'] + "/"

        # Check out experiment
        print("===============================")
        print('Checking out experiment branch...')
        subprocess.call(["git", "checkout", experiment['id']], cwd=app_path)

        # Get labels and add default ones
        labels = experiment['labels']
        labels['EXPERIMENT_ID'] = experiment['id']
        labels['EXPERIMENT_NAME'] = experiment['name']
        labels['APPLICATION_ID'] = app['id']
        labels['APPLICATION_NAME'] = app['name']
        labels['CPUS'] = str(cpus)
        labels['NODES'] = str(nodes)
        labels['TOTALCPUS'] = str(nodes * cpus)

        # List labels
        for label in labels.keys():
            key = "[[[{0}]]]".format(label)
            value = labels[label]
            print("Replacing: {0} <-- {1}".format(key, value))

        # Apply labels
        print("===============================")
        print("Applying labels...")
        for file in os.listdir(app_path):
            file = os.path.join(app_path, file)
            if os.path.isfile(file):
                self._replaceLabelsInFile(file, labels)

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
        subprocess.call(["git", "add", "*"], cwd=app_path)
        subprocess.call(["git", "commit", "-m", commit_msg], cwd=app_path)
        subprocess.call(["git", "checkout", "master"], cwd=app_path)


# Start RPC server
# Execute this only if called directly from python command
# From now RPC is waiting for requests
if __name__ == "__main__":
    rpc = zerorpc.Server(Storage(
        path="/home/devstack/datastorage",
        public_url="161.67.100.29",
        username="devstack"
    ))
    rpc.bind("tcp://0.0.0.0:8237")
    rpc.run()
