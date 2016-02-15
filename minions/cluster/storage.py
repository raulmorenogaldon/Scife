import os
import uuid
import shutil
import subprocess

class Storage(object):

    def __init__(self):
        # Create applications and experiments array
        self.applications = []
        self.experiments = []

        # Set path for storage
        self.path = "./datastorage"

        # Check if datastorage exists
        if os.path.isdir(self.path):
            return

        # Create storage folder
        os.mkdir(self.path)

    def uploadApplication(self, config):
        # Check if config is valid
        if not(os.path.isdir(config['path'])):
            raise IOError("Invalid input path, does not exists: {0}".format(config['path']))

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
        dst_path = self.path + "/" + id + "/" #+ os.path.basename(src_path)

        # Copy application to storage
        print('Copying app "{0}": {1} --> {2}'.format(config['name'], src_path, dst_path))
        shutil.copytree(src_path, dst_path)

        # Create git repository for this app
        print('Creating repository...')
        subprocess.call(["git", "init"], cwd=dst_path)
        subprocess.call(["git", "add", "*"], cwd=dst_path)
        subprocess.call(["git", "commit", "-m", "'Application created'"], cwd=dst_path)

        # Add application to DB
        self.applications.append(app)

        return app['id']

    def getApplication(self, app_id):
        # Search app
        for app in self.applications:
            if app['id'] == app_id:
                return app
        return None

    def createExperiment(self, name, app_id, script, labels):
        # Retrieve application
        app = self.getApplication(app_id)
        if app == None:
            raise Exception("Application ID does not exists")

        # Create experiment metadata
        experiment = {
            'id': str(uuid.uuid1()),
            'app': app['id'],
            'name': name,
            'labels': labels,
            'script': script
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
        print("===============================")
        comm = "./{0}".format(experiment['script'])
        print("Executing experiment creation script: {0}".format(comm))
        subprocess.call([comm], cwd=app_path)

        # Reapply labels
        print("===============================")
        print("Reapplying labels...")
        for file in os.listdir(app_path):
            file = os.path.join(app_path, file)
            if os.path.isfile(file):
                self._replaceLabelsInFile(file, experiment['labels'])

        # Commit changes and return to master
        print("===============================")
        print('Committing...')
        subprocess.call(["git", "add", "-u"], cwd=app_path)
        subprocess.call(["git", "commit", "-m", "Created experiment {0}".format(experiment['id'])], cwd=app_path)
        subprocess.call(["git", "checkout", "master"], cwd=app_path)



