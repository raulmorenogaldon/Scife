import os
import uuid
import shutil
import subprocess

class Storage(object):

    def __init__(self):
        # Create applications array
        self.applications = []

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
        dst_path = self.path + "/" + id + "/"#+ os.path.basename(src_path)

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

