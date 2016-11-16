import json
import getopt
import gevent.subprocess
import os
import glob
import re
import sys
import uuid
import zerorpc

import pymongo
from pymongo import MongoClient


class Storage(object):
    """Class to handle application storage in standard a FS."""

    def __init__(self, apppath, inputpath, outputpath, public_url, username, mongo, db):
        print("Initializing storage...")

        # Set path for storage
        self.apppath = apppath
        self.inputpath = inputpath
        self.outputpath = outputpath

        # Set user
        self.username = username

        # Set public url
        self.public_url = public_url

        # Connect to DB (default "localhost")
        # db vars must be private to avoid zerorpc errors
        print("Connecting to DB...")
        try:
            self._db_client = MongoClient(mongo)
            self._db = self._db_client[db]
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

        # Check if appstorage exists
        if not os.path.isdir(self.apppath):
            # Create storage folder
            os.mkdir(self.apppath)

        # Check if inputstorage exists
        if not os.path.isdir(self.inputpath):
            # Create storage folder
            os.mkdir(self.inputpath)

        # Check if outputstorage exists
        if not os.path.isdir(self.outputpath):
            # Create storage folder
            os.mkdir(self.outputpath)

        # Load apps
        print("Loading apps in DB...")
        cursor = self._db.applications.find()
        if cursor.count() == 0:
            print("No apps in DB!")
        else:
            for app in cursor:
                # Check if exists in folder
                app_path = os.path.join(self.apppath, app['id'])
                if os.path.isdir(app_path):
                    print("App loaded:", app['name'], app['id'])
                else:
                    print("App {0} - {1} not found!, removing from DB...".format(
                        app['name'], app['id']
                    ))
                    self._db.applications.delete_one({'id': app['id']})
        print("Initialization completed!")

    def copyApplication(self, app_id, src_path):
        # Check if config is valid
        if not(os.path.isdir(src_path)):
            raise IOError("Invalid input path, does not exists: {0}".format(
                src_path
            ))

        # Get destination path and create it
        dst_path = self.apppath + "/" + app_id

        # Copy application to storage
        print('Copying app "{0}": {1} --> {2}'.format(
            app_id, src_path, dst_path
        ))
        gevent.subprocess.call(["scp", "-r", src_path, dst_path])

        # Create input data folder
        input_path = self.inputpath + "/" + app_id
        os.mkdir(input_path)

        # Create git repository for this app
        print('Creating repository...')
        gevent.subprocess.call(["git", "init"], cwd=dst_path)
        gevent.subprocess.call(["git", "add", "*"], cwd=dst_path)
        gevent.subprocess.call(["git", "commit", "-q", "-m", "'Application created'"], cwd=dst_path)

    def discoverLabels(self, app_id):
        # Get application path
        app_path = self.apppath + "/" + app_id

        labels = []
        print('Discovering parameters...')
        for file in os.listdir(app_path):
            file = os.path.join(app_path, file)
            if os.path.isfile(file):
                labels = list(set(labels + self._getLabelsInFile(file)))
        return labels

    def copyExperiment(self, exp_id, app_id):
        # Get application repository path
        repo_path = self.apppath + "/" + app_id + "/"

        # Get application and experiment input storage path
        app_path = self.inputpath + "/" + app_id + "/"
        exp_path = self.inputpath + "/" + exp_id + "/"

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Create experiment branch
        print('Creating experiment branch...')
        gevent.subprocess.call(["git", "checkout", "master"], cwd=repo_path)
        gevent.subprocess.call(["git", "branch", "-D", exp_id], cwd=repo_path)
        gevent.subprocess.call(["git", "branch", exp_id], cwd=repo_path)

        # Copy inputs to storage
        print('Copying app default inputdata: {0} --> {1}'.format(
            app_path, exp_path
        ))
        gevent.subprocess.call(["cp", "-as", app_path, exp_path])

        self.lock = False
        ########################

    def retrieveExperimentOutput(self, exp_id, src_path):
        # Get experiment output storage path
        dst_path = self.outputpath + "/" + exp_id + "/"

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Copy output to storage
        print('Copying experiment output data: {0} --> {1}'.format(
            src_path, dst_path
        ))
        gevent.subprocess.call(["scp", src_path, dst_path])

        self.lock = False
        ########################

    def prepareExecution(self, app_id, exp_id, exec_id, labels):
        # Get application storage path
        app_path = self.apppath + "/" + app_id + "/"

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Create execution branch
        print('Creating execution branch...')
        gevent.subprocess.call(["git", "checkout", exp_id], cwd=app_path)
        gevent.subprocess.call(["git", "branch", "-D", exec_id], cwd=app_path)
        gevent.subprocess.call(["git", "branch", exec_id], cwd=app_path)

        # Apply parameters
        self._applyExperimentLabels(app_id, exp_id, exec_id, labels)

        # Create output data folder
        output_path = self.outputpath + "/" + exec_id
        if not os.path.isdir(output_path):
            os.mkdir(output_path)

        self.lock = False
        ########################
        return exp_id

    def removeExperiment(self, app_id, exp_id):
        # Get application storage path
        app_path = self.apppath + "/" + app_id + "/"

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Remove input storage path
        input_path = self.inputpath + "/" + exp_id
        gevent.subprocess.call(["rm", "-rf", input_path])

        # Remove experiment branch
        print('Removing experiment branch...')
        gevent.subprocess.call(["git", "branch", "-D", exp_id + "-L"], cwd=app_path)
        gevent.subprocess.call(["git", "branch", "-D", exp_id], cwd=app_path)

        self.lock = False
        ########################

    def getApplicationURL(self, app_id):
        # Get application storage path
        #app_path = self.apppath + "/" + app_id

        # Get public URL for this experiment
        #url = "{0}@{1}:{2}".format(self.username, self.public_url, app_path)
        url = "git://{0}/{1}".format(self.public_url, app_id)

        return url

    def getExecutionOutputURL(self, exp_id):
        # Get output storage path
        output_path = self.outputpath + "/" + exp_id

        # Get output storage path
        url = "{0}@{1}:{2}".format(self.username, self.public_url, output_path)
        return url

    def getExperimentInputURL(self, exp_id):
        # Get input storage path
        input_path = self.inputpath + "/" + exp_id

        # Get public URL for this experiment
        url = "{0}@{1}:{2}".format(self.username, self.public_url, input_path)

        return url

    def getExperimentCode(self, exp_id, app_id, fpath):
        # Get input storage path
        app_path = self.apppath + "/" + app_id

        # Get file
        try:
            fcontent = gevent.subprocess.check_output(
                ["git", "show", exp_id+":"+fpath], cwd=app_path)
        except:
            raise IOError("Not a file: '{0}'.".format(fpath))

        return fcontent

    def putExperimentCode(self, exp_id, app_id, fpath, fcontent):
        # Get input storage path
        app_path = self.apppath + "/" + app_id

        # Avoid absolute paths
        # i.e. /root
        if os.path.isabs(fpath):
            raise IOError("Delete is not supported for absolute paths: '{0}'".format(
                fpath
            ))

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Check out experiment
        gevent.subprocess.call(["git", "checkout", exp_id], cwd=app_path)

        # Check if path exists
        if not os.path.isdir(app_path):
            # Release lock
            self.lock = False
            raise IOError("Path '{0}' not found".format(
                fpath
            ))

        # Create new path
        gevent.subprocess.call(["mkdir", "-p", os.path.dirname(fpath)], cwd=app_path)

        if fcontent is not None:
            # File case
            try:
                f = open(app_path + "/" + fpath, 'w')
                f.write(fcontent)
                f.close()
            except Exception as e:
                self.lock = False
                raise e
            gevent.subprocess.call(["git", "add", fpath], cwd=app_path)
        else:
            # Keep empty folder
            gitkeep = os.path.dirname(fpath)+"/.gitkeep"
            gevent.subprocess.call(["touch", gitkeep], cwd=app_path)
            gevent.subprocess.call(["git", "add", gitkeep], cwd=app_path)

        # Commit
        gevent.subprocess.call(["git", "commit", "-m", "Commit..."], cwd=app_path)

        # Checkout master
        gevent.subprocess.call(["git", "checkout", "master"], cwd=app_path)

        self.lock = False
        ########################
        return

    def deleteExperimentCode(self, exp_id, app_id, fpath):
        # Get input storage path
        app_path = self.apppath + "/" + app_id

        # Avoid absolute paths
        # i.e. /root
        if os.path.isabs(fpath):
            raise IOError("Delete is not supported for absolute paths: '{0}'".format(
                fpath
            ))

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Check out experiment
        gevent.subprocess.call(["git", "checkout", exp_id], cwd=app_path)

        # Check if path exists
        if not os.path.isdir(app_path):
            # Release lock
            self.lock = False
            raise IOError("Path '{0}' not found".format(
                fpath
            ))

        if fpath is not None:
            gevent.subprocess.call(["rm", "-rf", fpath], cwd=app_path)
            gevent.subprocess.call(["git", "rm", "-r", fpath], cwd=app_path)
            gevent.subprocess.call(["git", "commit", "-m", "Commit..."], cwd=app_path)

        # Checkout master
        gevent.subprocess.call(["git", "checkout", "master"], cwd=app_path)

        self.lock = False
        ########################
        return

    def putExperimentInput(self, exp_id, app_id, fpath, src_path):
        # Get input storage path
        exp_path = self.inputpath + "/" + exp_id

        # Avoid absolute paths
        # i.e. /root
        if os.path.isabs(fpath):
            raise IOError("Delete is not supported for absolute paths: '{0}'".format(
                fpath
            ))

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Create new path
        gevent.subprocess.call(["mkdir", "-p", os.path.dirname(fpath)], cwd=exp_path)

        # Copy file
        if src_path is not None:
            gevent.subprocess.call(["scp", src_path, fpath], cwd=exp_path)

        self.lock = False
        ########################
        return

    def deleteExperimentInput(self, exp_id, app_id, fpath):
        # Get input storage path
        exp_path = self.inputpath + "/" + exp_id

        # Avoid absolute paths
        # i.e. /root
        if fpath is not None:
            if os.path.isabs(fpath):
                raise IOError("Delete is not supported for absolute paths: '{0}'".format(
                    fpath
                ))

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Remove file
        if fpath is not None:
            gevent.subprocess.call(["rm", "-rf", fpath], cwd=exp_path)
        else:
            # Remove folder
            gevent.subprocess.call(["rm", "-rf", exp_path])

        self.lock = False
        ########################
        return

    def getExecutionOutputFile(self, exp_id, fpath):
        # Check if none fpath
        if fpath is None:
            fpath = "output.tar.gz"

        # Get output storage path
        file = self.outputpath + "/" + exp_id + "/" + fpath

        # Check existence
        if not os.path.isfile(file):
            raise IOError("Output data '{0}' does not exist for experiment: {1}".format(
                fpath, exp_id
            ))

        return file

    def deleteExecutionOutput(self, exec_id, fpath):
        # Get output storage path
        exec_path = self.outputpath + "/" + exec_id

        # Avoid absolute paths
        # i.e. /root
        if fpath is not None:
            if os.path.isabs(fpath):
                raise IOError("Delete is not supported for absolute paths: '{0}'".format(
                    fpath
                ))

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Remove file
        if fpath is not None:
            gevent.subprocess.call(["rm", "-rf", fpath], cwd=exec_path)
        else:
            # Remove folder
            gevent.subprocess.call(["rm", "-rf", exec_path])

        self.lock = False
        ########################
        return

    def getOutputFolderUsage(self, id):
        # Get input storage path
        path = self.outputpath + "/" + id

        # Check if output exists
        if not os.path.isdir(path):
            return None

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Get bytes
        bytes = getFolderSize(path)

        self.lock = False
        ########################

        return bytes

    def getInputIDs(self):
        return os.listdir(self.inputpath)

    def getOutputIDs(self):
        return os.listdir(self.outputpath)

    def getOutputFolderTree(self, id):
        # Get input storage path
        path = self.outputpath + "/" + id

        # Check if output exists
        if not os.path.isdir(path):
            return []

        # Create tree
        return self._fillFolderTree(path, "")

    def getInputFolderTree(self, id):
        # Get input storage path
        path = self.inputpath + "/" + id

        # Create tree
        return self._fillFolderTree(path, "")

    def getExperimentSrcFolderTree(self, exp_id, app_id):
        # Get input storage path
        root = self.apppath + "/" + app_id

        ########################
        # Wait for the lock
        while self.lock:
            gevent.sleep(0)
        self.lock = True

        # Check out experiment
        gevent.subprocess.call(["git", "checkout", exp_id], cwd=root)

        # Get tree
        tree = self._fillFolderTree(root, "")

        # Checkout master
        gevent.subprocess.call(["git", "checkout", "master"], cwd=root)

        self.lock = False
        ########################
        return tree

    def _fillFolderTree(self, root, rel_path):
        # Iterate current level tree
        tree = []
        dir = os.path.join(root, rel_path)
        for file in os.listdir(dir):
            if file[0] != '.':
                full_filepath = os.path.join(dir, file)
                rel_filepath = os.path.join(rel_path, file)
                if os.path.isfile(full_filepath):
                    # Add leaf
                    tree.append({
                        "label": file,
                        "id": rel_filepath,
                        "children": []
                    })
                else:
                    # Add subtree
                    tree.append({
                        "label": file,
                        "id": rel_filepath+"/",
                        "children": self._fillFolderTree(root, rel_filepath)
                    })
        return tree

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

    def _applyExperimentLabels(self, app_id, exp_id, exec_id, labels):
        # Get application storage path
        app_path = self.apppath + "/" + app_id + "/"

        # Check out experiment
        print("===============================")
        print('Checking out experiment launch branch...')
        gevent.subprocess.call(["git", "checkout", exec_id], cwd=app_path)

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

        # Commit changes and return to master
        print("===============================")
        print('Committing...')
        commit_msg = "Launched execution {0}".format(exec_id)
        gevent.subprocess.call(["git", "add", "*"], cwd=app_path)
        gevent.subprocess.call(["git", "commit", "-m", commit_msg], cwd=app_path)
        gevent.subprocess.call(["git", "checkout", "master"], cwd=app_path)


# http://stackoverflow.com/questions/1392413/calculating-a-directory-size-using-python
# Samuel Lampa
def getFolderSize(folder):
    total_size = os.path.getsize(folder)
    for item in os.listdir(folder):
        itempath = os.path.join(folder, item)
        if os.path.isfile(itempath):
            total_size += os.path.getsize(itempath)
        elif os.path.isdir(itempath):
            total_size += getFolderSize(itempath)
    return total_size

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
        apppath=cfg['appstorage'],
        inputpath=cfg['inputstorage'],
        outputpath=cfg['outputstorage'],
        public_url=cfg['public_url'],
        username=cfg['username'],
        mongo=cfg['mongo'],
        db=cfg['db']
    ), heartbeat=30)
    print "Listening in {0}".format(cfg['listen'])
    rpc.bind(cfg['listen'])
    rpc.run()
