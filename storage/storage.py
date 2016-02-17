import os
import re
import uuid
import shutil
import subprocess


class Storage(object):
    """Class to handle application storage in standard a FS."""

    def __init__(self, path, public_url, username):
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
            return

        # Create storage folder
        os.mkdir(self.path)

    def createApplication(self, config):
        # Check if config is valid
        if not(os.path.isdir(config['path'])):
            raise IOError("Invalid input path, does not exists: {0}".format(
                config['path']
            ))

        # Create UUID for application
        id = str(uuid.uuid1())

        # Create application data
        app = {
            'name': config["name"],
            'id': id,
        }

        # Get source path
        src_path = config['path']

        # Get destination path and create it
        dst_path = self.path + "/" + id + "/"

        # Copy application to storage
        print('Copying app "{0}": {1} --> {2}'.format(
            config['name'], src_path, dst_path
        ))
        shutil.copytree(src_path, dst_path)

        # Create labels list
        print('Discovering parameters...')
        for file in os.listdir(dst_path):
            file = os.path.join(dst_path, file)
            if os.path.isfile(file):
                app['labels'] = self._getLabelsInFile(file)

        # Create git repository for this app
        print('Creating repository...')
        subprocess.call(["git", "init"], cwd=dst_path)
        subprocess.call(["git", "add", "*"], cwd=dst_path)
        subprocess.call(["git", "commit", "-m", "'Application created'"], cwd=dst_path)

        # Add application to DB
        self.applications.append(app)

        return app['id']

    def _getLabelsInFile(self, file):
        print("Getting labels from file: {0}".format(file))
        # Load file
        f = open(file, 'r')
        filedata = f.read()
        f.close()
        # Find labels
        labels = re.findall(r"\[\[\[(\w+)\]\]\]", filedata)
        print("Found: {0}".format(labels))
        return labels

    def getApplication(self, app_id):
        # Search app
        for app in self.applications:
            if app['id'] == app_id:
                return app
        return None

    def createExperiment(self, name, app_id, creation_script, exe_script, labels):
        # Retrieve application
        app = self.getApplication(app_id)
        if app is None:
            raise Exception("Application ID does not exists")

        # Create experiment metadata
        experiment = {
            'id': str(uuid.uuid1()),
            'app': app['id'],
            'name': name,
            'labels': labels,
            'creation_script': creation_script,
            'execution_script': exe_script
        }

        # Get application storage path
        app_path = self.path + "/" + app['id'] + "/"

        # Create experiment branch
        print('Creating experiment branch...')
        subprocess.call(["git", "branch", experiment['id']], cwd=app_path)

        # Apply parameters
        self._applyExperimentParams(app, experiment)

        self.experiments.append(experiment)
        return experiment['id']

    def getExperimentPublicURL(self, experiment):
        # Get application storage path
        app_path = self.path + "/" + experiment['app']

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
            print("Replacing: {0} <-- {1}".format(key, value))
            filedata = filedata.replace(key, value)
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
        subprocess.call(["git", "checkout", experiment['id']], cwd=app_path)

        # Apply labels
        print("===============================")
        print("Applying labels...")
        for file in os.listdir(app_path):
            file = os.path.join(app_path, file)
            if os.path.isfile(file):
                self._replaceLabelsInFile(file, experiment['labels'])

        # Execute experiment creation script
        if experiment['creation_script'] is not None:
            print("===============================")
            comm = "./{0}".format(experiment['creation_script'])
            print("Executing experiment creation script: {0}".format(comm))
            subprocess.call([comm], cwd=app_path)

        # Commit changes and return to master
        print("===============================")
        print('Committing...')
        commit_msg = "Created experiment {0}".format(experiment['id'])
        subprocess.call(["git", "add", "-u"], cwd=app_path)
        subprocess.call(["git", "commit", "-m", commit_msg], cwd=app_path)
        subprocess.call(["git", "checkout", "master"], cwd=app_path)


# Start RPC server
# Execute this only if called directly from python command
# From now RPC is waiting for requests
if __name__ == "__main__":
    rpc = zerorpc.Server(Storage())
    rpc.bind("tcp://0.0.0.0:4242")
    rpc.run()
